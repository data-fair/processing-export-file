import fs from 'node:fs'
import { Writable } from 'node:stream'
import type { Writable as WritableT } from 'node:stream'
import { stringify as csvStringify } from 'csv-stringify'
import pkg from '@dsnp/parquetjs'
import Excel from 'exceljs'
import type { Field } from './transforms.ts'
import { buildParquetSchemaDefinition } from './parquet-schema.ts'

const { ParquetSchema, ParquetTransformer } = pkg

export function getCsvPipeline (filePath: string, fields: Field[]): WritableT[] {
  const columns = fields.length ? fields.map(f => f.key) : undefined
  if (columns?.includes('_geopoint')) {
    columns.push('latitude')
    columns.push('longitude')
  }
  return [
    csvStringify({
      header: true,
      quoted_string: true,
      cast: { boolean: (value: boolean) => value ? '1' : '0' },
      ...(columns ? { columns } : {})
    }),
    fs.createWriteStream(filePath, { flags: 'w' })
  ]
}

export function getParquetPipeline (filePath: string, fields: Field[]): WritableT[] {
  const schema = new ParquetSchema(buildParquetSchemaDefinition(fields) as any)
  return [
    // the footer metadata of every row group is kept in memory until the file is closed,
    // so the defaults (rowGroupSize 4096 + pageIndex) grow the heap linearly with the row count
    new ParquetTransformer(schema, { rowGroupSize: 20000, pageIndex: false }),
    fs.createWriteStream(filePath, { flags: 'w' })
  ]
}

const maxPendingChunks = 32
const resumePendingChunks = 8

export interface XlsxBackpressure {
  /** resolves once the zip stream has caught up enough to accept more rows */
  wait: () => Promise<void>
  /** unblocks every waiter, to avoid hanging when the underlying stream dies */
  release: () => void
}

/**
 * exceljs gives us no backpressure: StreamBuf.write() always returns true (the library even
 * says so in a comment) and fires its 64kB buffers into the zip stream without awaiting them.
 * Rows written faster than the zip compresses them therefore pile up off-heap, ~900MB of
 * buffers for 1M rows. Counting the chunks still in flight lets us pause the source instead,
 * which divides the resident memory of a 1M rows export by about 4.
 *
 * /!\ This wraps StreamBuf._pipe, a private member of exceljs (lib/utils/stream-buf.js).
 * It is only ever read here and in test-it/xlsx-backpressure.test.ts, which fails if a future
 * version of exceljs renames it — without that test the export would silently go back to
 * buffering everything in memory. Returns undefined rather than throwing, so a broken hook
 * degrades to the previous behaviour instead of breaking the export.
 */
export function hookXlsxBackpressure (worksheet: any): XlsxBackpressure | undefined {
  const streamBuf = worksheet?.stream
  if (typeof streamBuf?._pipe !== 'function') return undefined

  const pipe = streamBuf._pipe.bind(streamBuf)
  let pending = 0
  let waiters: (() => void)[] = []
  const release = () => {
    const resuming = waiters
    waiters = []
    for (const resume of resuming) resume()
  }

  streamBuf._pipe = (chunk: any) => {
    pending++
    return pipe(chunk).finally(() => {
      pending--
      if (pending <= resumePendingChunks) release()
    })
  }

  return {
    wait: async () => {
      if (pending <= maxPendingChunks) return
      await new Promise<void>((resolve) => waiters.push(resolve))
    },
    release
  }
}

export function getXlsxPipeline (filePath: string, fields: Field[], label: string): WritableT[] {
  const writeStreamXlsx = fs.createWriteStream(filePath, { flags: 'w' })
  const workbook = new Excel.stream.xlsx.WorkbookWriter({ stream: writeStreamXlsx })
  const worksheet = workbook.addWorksheet(label)
  worksheet.columns = fields.map(f => ({ header: f.key, key: f.key }))

  const backpressure = hookXlsxBackpressure(worksheet)
  if (backpressure) writeStreamXlsx.once('error', backpressure.release)

  const writable = new Writable({
    objectMode: true,
    write (line, _enc, next) {
      worksheet.addRow(line).commit()
      if (!backpressure) return next()
      backpressure.wait().then(() => next(), next)
    },
    final (callback) {
      worksheet.commit()
      workbook.commit().then(() => callback()).catch(callback)
    }
  })

  return [writable]
}
