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

export function getXlsxPipeline (filePath: string, fields: Field[], label: string): WritableT[] {
  const writeStreamXlsx = fs.createWriteStream(filePath, { flags: 'w' })
  const workbook = new Excel.stream.xlsx.WorkbookWriter({ stream: writeStreamXlsx })
  const worksheet = workbook.addWorksheet(label)
  worksheet.columns = fields.map(f => ({ header: f.key, key: f.key }))

  const writable = new Writable({
    objectMode: true,
    write (line, _enc, next) {
      worksheet.addRow(line).commit()
      next()
    },
    final (callback) {
      worksheet.commit()
      workbook.commit().then(() => callback()).catch(callback)
    }
  })

  return [writable]
}
