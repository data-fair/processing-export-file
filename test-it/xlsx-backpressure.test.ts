import fs from 'node:fs'
import path from 'node:path'
import { strict as assert } from 'node:assert'
import { after, before, describe, it } from 'node:test'
import { finished } from 'node:stream/promises'
import Excel from 'exceljs'
import { getXlsxPipeline, hookXlsxBackpressure } from '../lib/pipelines.ts'

const TMP = 'data/tmp-xlsx-backpressure'
const fields = Array.from({ length: 20 }, (_, i) => ({ key: 'f' + i, type: 'string' }))
const makeLine = (i: number) => Object.fromEntries(fields.map(f => [f.key, `${f.key}-value-${i}`]))

const newWorksheet = (filePath: string) => {
  const workbook = new Excel.stream.xlsx.WorkbookWriter({ stream: fs.createWriteStream(filePath, { flags: 'w' }) })
  const worksheet = workbook.addWorksheet('Export')
  worksheet.columns = fields.map(f => ({ header: f.key, key: f.key }))
  return { workbook, worksheet }
}

describe('export-file — xlsx backpressure', () => {
  before(() => {
    fs.mkdirSync(TMP, { recursive: true })
  })

  after(() => {
    fs.rmSync(TMP, { recursive: true, force: true })
  })

  // This is the canary for the private exceljs member the hook depends on. If it ever fails
  // after an exceljs upgrade, the xlsx export silently goes back to buffering the whole zip
  // in memory (~900MB of buffers for 1M rows): find the new backpressure hook, do not delete.
  it('exceljs still exposes the private StreamBuf._pipe the hook depends on', () => {
    const { worksheet } = newWorksheet(path.join(TMP, 'canary.xlsx'))
    assert.ok(
      hookXlsxBackpressure(worksheet),
      'exceljs no longer exposes worksheet.stream._pipe, the xlsx export is no longer throttled'
    )
  })

  it('blocks the source while too many chunks are in flight, then resumes', async () => {
    const { worksheet } = newWorksheet(path.join(TMP, 'gate.xlsx'))
    const backpressure = hookXlsxBackpressure(worksheet)
    assert.ok(backpressure)

    // nothing in flight yet, writing must not be throttled
    assert.equal(await Promise.race([backpressure.wait().then(() => 'free'), Promise.resolve('free')]), 'free')

    // enough rows to push well past the in-flight chunk threshold
    for (let i = 0; i < 20000; i++) worksheet.addRow(makeLine(i)).commit()

    const blocked = backpressure.wait()
    const raced = await Promise.race([
      blocked.then(() => 'resumed'),
      new Promise((resolve) => setImmediate(() => resolve('blocked')))
    ])
    assert.equal(raced, 'blocked', 'wait() should hold the source back while the zip catches up')

    await blocked
  })

  it('writes every row through the throttled pipeline', async () => {
    const filePath = path.join(TMP, 'full.xlsx')
    const [writable] = getXlsxPipeline(filePath, fields, 'Export')
    const done = finished(writable)
    for (let i = 0; i < 20000; i++) {
      if (!writable.write(makeLine(i))) await new Promise<void>((resolve) => writable.once('drain', () => resolve()))
    }
    writable.end()
    await done

    let lastRow = 0
    for await (const worksheet of new Excel.stream.xlsx.WorkbookReader(filePath, {})) {
      for await (const row of worksheet) lastRow = row.number
    }
    assert.equal(lastRow, 20001, 'header + 20000 rows were written')
  })
})
