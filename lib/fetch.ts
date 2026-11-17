import type { AxiosInstance } from 'axios'
import type { Writable } from 'node:stream'
import { parse as csvParse } from 'csv-parse/sync'
import type { LogFunctions } from '@data-fair/lib-common-types/processings.js'
import { transformLine, type Field, type GeomField } from './transforms.ts'

export interface FetchAndWriteOpts {
  datasetHref: string
  fields: Field[]
  filters: any[] | undefined
  geomField: GeomField | undefined
  axios: AxiosInstance
  log: LogFunctions
  writeStreams: Writable[]
  isStopped: () => boolean
  pageSize?: number
}

async function buildLinesUrl (datasetHref: string, fields: Field[], filters: any[] | undefined, size: number): Promise<URL> {
  const { filters2qs } = await import('@data-fair/lib-utils/filters/index.js')
  const urlObj = new URL(datasetHref + '/lines')
  urlObj.searchParams.set('size', String(size))
  if (fields.length) urlObj.searchParams.set('select', fields.map(f => f.key).join(','))
  if (filters?.length) urlObj.searchParams.set('qs', filters2qs(filters))
  return urlObj
}

/**
 * Number of lines the export will actually contain, filters included.
 */
export async function countLines (datasetHref: string, filters: any[] | undefined, axios: AxiosInstance): Promise<number> {
  const url = await buildLinesUrl(datasetHref, [], filters, 0)
  const { data }: { data: any } = await axios(url.href)
  return data.total
}

const csvTypes = new Set(['string', 'integer', 'number', 'boolean'])

/**
 * data-fair throttles /lines on the bytes it sends, and a JSON page repeats every column name on
 * every line: the same page is about 3 times smaller as CSV. The CSV of data-fair writes strings
 * quoted, booleans as 1/0 and null as an unquoted empty cell, so the typed values can be rebuilt.
 * Objects (geoshape) are flattened differently in both formats, they keep the JSON transport.
 */
export const canFetchCsv = (fields: Field[]): boolean =>
  fields.length > 0 && fields.every(f => !!f.type && csvTypes.has(f.type) && !f.key.startsWith('_geoshape'))

/** Lines of a CSV page of /lines (header=false), typed like the JSON transport returns them. */
export const parseCsvPage = (text: string, fields: Field[]): Record<string, any>[] => {
  const types = fields.map(f => f.type)
  return csvParse(text, {
    bom: true,
    columns: fields.map(f => f.key),
    cast: (value, context) => {
      // a null value is an unquoted empty cell, an empty string is quoted
      if (value === '' && !context.quoting) return undefined
      const type = types[context.index]
      if (type === 'integer' || type === 'number') return Number(value)
      if (type === 'boolean') return value === '1'
      return value
    }
  })
}

const nextFromLinkHeader = (link: string | undefined): string | undefined =>
  link?.match(/<([^>]+)>\s*;\s*rel="?next"?/)?.[1]

export async function fetchAndWriteData (opts: FetchAndWriteOpts): Promise<void> {
  const { datasetHref, fields, filters, geomField, axios, log, writeStreams, isStopped, pageSize = 10000 } = opts

  const csv = canFetchCsv(fields)
  const urlObj = await buildLinesUrl(datasetHref, fields, filters, pageSize)
  if (csv) {
    urlObj.searchParams.set('format', 'csv')
    urlObj.searchParams.set('header', 'false')
  }
  let url: string | undefined = urlObj.href

  await log.task('Downloading the dataset lines')
  // the CSV pages carry no total, and the JSON ones only on the first page
  let total = csv ? await countLines(datasetHref, filters, axios) : undefined
  let count = 0

  try {
    while (url) {
      if (isStopped()) return
      const res = csv ? await axios(url, { responseType: 'text' }) : await axios(url)
      let results: Record<string, any>[]
      if (csv) {
        results = parseCsvPage(res.data, fields)
        url = nextFromLinkHeader(res.headers?.link)
      } else {
        results = res.data.results
        url = res.data.next
        total ??= res.data.total
      }
      // the next link comes from the response, only follow it on the dataset's own origin
      if (url && new URL(url).origin !== urlObj.origin) throw new Error(`The next page link ${url} is not on the origin of the dataset (${urlObj.origin}), it is not followed`)
      for (const line of results) {
        transformLine(line, fields, geomField)
        for (const writeStream of writeStreams) {
          const keepWriting = writeStream.write(line)
          if (!keepWriting) await new Promise<void>((resolve) => writeStream.once('drain', () => resolve()))
        }
      }
      count += results.length
      await log.progress('Downloading the dataset lines', count, total)
    }
  } finally {
    for (const writeStream of writeStreams) writeStream.end()
  }
}
