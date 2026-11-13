import fs from 'node:fs'
import path from 'node:path'
import { PassThrough } from 'node:stream'
import { strict as assert } from 'node:assert'
import { after, before, describe, it } from 'node:test'
import { uploadAttachment } from '../lib/upload.ts'

const TMP = 'data/tmp-upload'
const log: any = { task: async () => {}, progress: async () => {}, info: async () => {}, warning: async (msg: string) => { warnings.push(msg) } }
const warnings: string[] = []

// form-data is an old style stream, it has to be piped
const readBody = (data: any): Promise<string> => new Promise((resolve, reject) => {
  const chunks: Buffer[] = []
  const collector = new PassThrough()
  collector.on('data', (chunk: Buffer) => chunks.push(chunk))
  collector.on('end', () => resolve(Buffer.concat(chunks).toString()))
  collector.on('error', reject)
  data.pipe(collector)
})

describe('uploadAttachment', () => {
  before(() => { fs.mkdirSync(TMP, { recursive: true }) })
  after(() => { fs.rmSync(TMP, { recursive: true, force: true }) })

  it('sends the whole file again after a gateway error, then patches the attachments', async () => {
    const filePath = path.join(TMP, 'export.csv')
    fs.writeFileSync(filePath, '"name"\n"alice"\n')
    const calls: { method: string, body?: string, data?: any }[] = []
    const axios: any = async (req: any) => {
      if (req.method === 'post') {
        const body = await readBody(req.data)
        calls.push({ method: 'post', body: body.replace(/-{2,}[0-9a-f]+/g, '--boundary') })
        // the shape of a processing-dev error, with the header the worker keeps
        if (calls.length === 1) throw Object.assign(new Error('unavailable'), { status: 503, headers: { 'retry-after': '1' } })
        return { data: { name: 'export.csv', size: 15 } }
      }
      calls.push({ method: req.method, data: req.data })
      return { data: {} }
    }
    const dataset = { attachments: [{ name: 'export.csv', title: 'old' }, { name: 'other.pdf' }] }

    await uploadAttachment({ filePath, format: 'csv', dataset, datasetHref: 'http://df/api/v1/datasets/x', label: 'Export', axios, log })

    assert.deepEqual(calls.map(c => c.method), ['post', 'post', 'patch'])
    assert.match(calls[0].body!, /alice/)
    assert.equal(calls[1].body, calls[0].body, 'the retried form carries the same file')
    assert.match(warnings[0], /answered 503, retrying in 1 s/)
    assert.deepEqual(calls[2].data.attachments.map((a: any) => a.name), ['other.pdf', 'export.csv'])
    assert.equal(calls[2].data.attachments[1].title, 'Export (csv)')
  })
})
