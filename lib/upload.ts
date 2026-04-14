import fs from 'node:fs'
import path from 'node:path'
import util from 'node:util'
import FormData from 'form-data'
import type { AxiosInstance } from 'axios'
import type { LogFunctions } from '@data-fair/lib-common-types/processings.js'

export interface UploadOpts {
  filePath: string
  dataset: any
  datasetHref: string
  label: string
  axios: AxiosInstance
  log: LogFunctions
}

export async function uploadAttachment (opts: UploadOpts): Promise<void> {
  const { filePath, dataset, datasetHref, label, axios, log } = opts
  const filename = path.parse(filePath).base
  const formData = new FormData()
  formData.append('attachment', fs.createReadStream(filePath), { filename })
  const getLengthAsync = util.promisify(formData.getLength).bind(formData)
  const contentLength = await getLengthAsync()

  const { formatBytes } = await import('@data-fair/lib-utils/format/bytes.js')
  const taskName = `Chargement de la pièce jointe ${filename} (${formatBytes(contentLength)})`
  await log.task(taskName)

  const response = await axios({
    method: 'post',
    url: `${datasetHref}/metadata-attachments`,
    data: formData,
    maxContentLength: Infinity,
    maxBodyLength: Infinity,
    headers: { ...formData.getHeaders(), 'content-length': contentLength },
    onUploadProgress: (progressEvent: any) => {
      log.progress(taskName, progressEvent.loaded, progressEvent.total).catch(() => {})
    }
  })

  await log.info('Mise à jour des métadonnées')

  const attachments = dataset.attachments ?? []
  const idx = attachments.findIndex((a: { name: string }) => a.name === filename)
  if (idx >= 0) attachments.splice(idx, 1)
  dataset.attachments = [
    ...attachments,
    {
      ...response.data,
      type: 'file',
      title: `${label} (${filename.split('.').pop()})`
    }
  ]

  await axios({
    method: 'patch',
    url: datasetHref,
    data: { attachments: dataset.attachments }
  })
}
