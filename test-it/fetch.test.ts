import { strict as assert } from 'node:assert'
import { describe, it } from 'node:test'
import { Writable } from 'node:stream'
import { fetchAndWriteData } from '../lib/fetch.ts'

const log: any = { task: async () => {}, progress: async () => {} }

describe('fetchAndWriteData', () => {
  it('does not follow a next link to another origin', async () => {
    const urls: string[] = []
    const axios: any = async (url: string) => {
      urls.push(url)
      return { data: { total: 2, results: [{ a: 1 }], next: 'https://evil.example.com/lines?page=2' } }
    }
    const writeStream = new Writable({ objectMode: true, write (_chunk, _enc, cb) { cb() } })
    await assert.rejects(fetchAndWriteData({
      datasetHref: 'https://df.example.com/data-fair/api/v1/datasets/ds1',
      fields: [],
      filters: undefined,
      geomField: undefined,
      axios,
      log,
      writeStreams: [writeStream],
      isStopped: () => false
    }), /unexpected next link/)
    assert.equal(urls.length, 1)
  })
})
