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

export async function fetchAndWriteData (opts: FetchAndWriteOpts): Promise<void> {
  const { datasetHref, fields, filters, geomField, axios, log, writeStreams, isStopped, pageSize = 10000 } = opts
  const { filters2qs } = await import('@data-fair/lib-utils/filters/index.js')

  const urlObj = new URL(datasetHref + '/lines')
  urlObj.searchParams.set('size', String(pageSize))
  if (fields.length) urlObj.searchParams.set('select', fields.map(f => f.key).join(','))
  if (filters?.length) urlObj.searchParams.set('qs', filters2qs(filters))

  let url: string | undefined = urlObj.href

  await log.task('Téléchargement des données')
  let count = 0

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
    await log.progress('Téléchargement des données', count, data.total)
  }
  for (const writeStream of writeStreams) writeStream.end()
}
