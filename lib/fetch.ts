import type { AxiosInstance } from 'axios'
import type { Writable } from 'node:stream'
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

export async function fetchAndWriteData (opts: FetchAndWriteOpts): Promise<void> {
  const { datasetHref, fields, filters, geomField, axios, log, writeStreams, isStopped, pageSize = 10000 } = opts

  const urlObj = await buildLinesUrl(datasetHref, fields, filters, pageSize)
  let url: string | undefined = urlObj.href

  await log.task('Downloading data')
  let count = 0

  try {
    while (url) {
      if (isStopped()) return
      const { data }: { data: any } = await axios(url)
      url = data.next
      for (const line of data.results) {
        transformLine(line, fields, geomField)
        for (const writeStream of writeStreams) {
          const keepWriting = writeStream.write(line)
          if (!keepWriting) await new Promise<void>((resolve) => writeStream.once('drain', () => resolve()))
        }
      }
      count += data.results.length
      await log.progress('Downloading data', count, data.total)
    }
  } finally {
    for (const writeStream of writeStreams) writeStream.end()
  }
}
