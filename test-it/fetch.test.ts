import { strict as assert } from 'node:assert'
import { describe, it } from 'node:test'
import { Writable } from 'node:stream'
import { canFetchCsv, fetchAndWriteData, parseCsvPage } from '../lib/fetch.ts'

const log: any = { task: async () => {}, progress: async () => {} }

const collector = () => {
  const lines: any[] = []
  const writeStream = new Writable({ objectMode: true, write (chunk, _enc, cb) { lines.push(chunk); cb() } })
  return { lines, writeStream }
}

const datasetHref = 'https://df.example.com/data-fair/api/v1/datasets/ds1'
const typedFields = [
  { key: 's', type: 'string' },
  { key: 'n', type: 'number' },
  { key: 'i', type: 'integer' },
  { key: 'b', type: 'boolean' }
]

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
    }), /next page link .* is not on the origin of the dataset/)
    assert.equal(urls.length, 1)
  })

  it('downloads typed fields as CSV pages and follows the Link header', async () => {
    const urls: string[] = []
    const axios: any = async (url: string, config?: any) => {
      urls.push(url)
      if (new URL(url).searchParams.get('size') === '0') return { data: { total: 3 } }
      assert.equal(config?.responseType, 'text')
      if (urls.filter(u => u.includes('format=csv')).length === 1) {
        return {
          headers: { link: `<${datasetHref}/lines?size=10000&format=csv&header=false&after=2>; rel="next"` },
          data: '\ufeff"a ""quoted"", text",1.5,42,1\n"",,,0\n'
        }
      }
      return { headers: {}, data: '\ufeff"last",-3,,\n' }
    }
    const { lines, writeStream } = collector()
    await fetchAndWriteData({ datasetHref, fields: typedFields, filters: undefined, geomField: undefined, axios, log, writeStreams: [writeStream], isStopped: () => false })
    assert.deepEqual(lines, [
      { s: 'a "quoted", text', n: 1.5, i: 42, b: true },
      { s: '', n: undefined, i: undefined, b: false },
      { s: 'last', n: -3, i: undefined, b: undefined }
    ])
    const first = new URL(urls[1])
    assert.equal(first.searchParams.get('format'), 'csv')
    assert.equal(first.searchParams.get('header'), 'false')
    assert.equal(first.searchParams.get('select'), 's,n,i,b')
    assert.ok(urls[2].includes('after=2'))
  })

  it('keeps the JSON transport for untyped fields and geoshapes', () => {
    assert.equal(canFetchCsv(typedFields), true)
    assert.equal(canFetchCsv([]), false)
    assert.equal(canFetchCsv([{ key: 'g' }]), false)
    assert.equal(canFetchCsv([{ key: '_geoshape', type: 'string' }]), false)
  })

  it('parses a CSV page without header', () => {
    assert.deepEqual(parseCsvPage('"x",2\n', [{ key: 'a', type: 'string' }, { key: 'b', type: 'integer' }]), [{ a: 'x', b: 2 }])
  })
})
