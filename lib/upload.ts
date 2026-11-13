import fs from 'node:fs'
import path from 'node:path'
import util from 'node:util'
import FormData from 'form-data'
import type { AxiosInstance } from 'axios'
import type { LogFunctions } from '@data-fair/lib-common-types/processings.js'
import { dfRetry } from './retry.ts'

export interface UploadOpts {
  filePath: string
  format: string
  dataset: any
  datasetHref: string
  label: string
  axios: AxiosInstance
  log: LogFunctions
}

export async function uploadAttachment (opts: UploadOpts): Promise<void> {
  const { filePath, format, dataset, datasetHref, label, axios, log } = opts
  const filename = path.parse(filePath).base
  // the file stream can only be read once, the form is rebuilt on every attempt
  const buildForm = () => {
    const formData = new FormData()
    formData.append('attachment', fs.createReadStream(filePath), { filename })
    return formData
  }
  const getLength = (formData: FormData): Promise<number> => util.promisify(formData.getLength).bind(formData)()
  let nextForm: FormData | undefined = buildForm()

  const { formatBytes } = await import('@data-fair/lib-utils/format/bytes.js')
  const taskName = `Uploading attachment ${filename} (${formatBytes(await getLength(nextForm))})`
  await log.task(taskName)

  const attachmentsUrl = `${datasetHref}/metadata-attachments`
  // an attachment is replaced by name, sending it again after a gateway error is harmless
  const response = await dfRetry(async () => {
    const formData = nextForm ?? buildForm()
    nextForm = undefined
    const contentLength = await getLength(formData)
    return await axios({
      method: 'post',
      url: attachmentsUrl,
      data: formData,
      maxContentLength: Infinity,
      maxBodyLength: Infinity,
      headers: { ...formData.getHeaders(), 'content-length': contentLength },
      onUploadProgress: (progressEvent: any) => {
        log.progress(taskName, progressEvent.loaded, progressEvent.total).catch(() => {})
      }
    })
  }, log, { label: `POST ${attachmentsUrl}` })

  await log.info(`Updating the attachments list of the dataset with ${filename}`)

  const attachments = dataset.attachments ?? []
  const idx = attachments.findIndex((a: { name: string }) => a.name === filename)
  if (idx >= 0) attachments.splice(idx, 1)
  dataset.attachments = [
    ...attachments,
    {
      ...response.data,
      type: 'file',
      title: `${label} (${format})`
    }
  ]

  await dfRetry(() => axios({
    method: 'patch',
    url: datasetHref,
    data: { attachments: dataset.attachments }
  }), log, { label: `PATCH ${datasetHref}` })
}
