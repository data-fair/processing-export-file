const fs = require('node:fs')
const path = require('node:path')
const { Writable } = require('node:stream')
const { pipeline, finished } = require('node:stream/promises')
const util = require('node:util')
const FormData = require('form-data')

let stopped = false

/**
 * @param {string} filePath
 * @param {any} processingConfig
 * @returns {import('node:stream').Writable[]}
 */
function getCSVStreamPipeline (filePath, processingConfig) {
  const csv = require('csv-stringify')
  return [
    csv.stringify({
      header: true,
      quoted_string: true,
      columns: processingConfig.fields.map((/** @type {{key: string}} */field) => field.key),
      cast: { boolean: value => value ? '1' : '0' }
    }),
    fs.createWriteStream(filePath, { flags: 'w' })
  ]
}

/**
 * @param {string} filePath
 * @param {any} processingConfig
 * @returns {import('node:stream').Writable[]}
 */
function getParquetStreamPipeline (filePath, processingConfig) {
  const { ParquetSchema, ParquetTransformer } = require('@dsnp/parquetjs')

  /** @type {Record<string, string>} */
  const typeConversion = {
    integer: 'INT64',
    number: 'FLOAT',
    string: 'UTF8',
    boolean: 'BOOLEAN'
  }

  /** @type {Record<string, any>} */
  const schemaDefinition = {}
  processingConfig.fields.forEach((/** @type {{key: string, type: string}} */column) => {
    schemaDefinition[column.key] = { type: typeConversion[column.type], optional: true }
  })
  const schema = new ParquetSchema(schemaDefinition)

  return [
    new ParquetTransformer(schema),
    fs.createWriteStream(filePath, { flags: 'w' })
  ]
}

/**
 * @param {string} filePath
 * @param {any} processingConfig
 * @returns {import('node:stream').Writable[]}
 */
function getXlsxStreamPipeline (filePath, processingConfig) {
  const Excel = require('exceljs')
  // const readableStreamXlsx = new ReadableStreamClone(readableStream, { objectMode: true })
  const writeStreamXlsx = fs.createWriteStream(filePath, { flags: 'w' })

  const workbook = new Excel.stream.xlsx.WorkbookWriter({ stream: writeStreamXlsx })
  const worksheet = workbook.addWorksheet(processingConfig.label)

  const columns = processingConfig.fields.map((/** @type {{key: string}} */field) => ({ header: field.key, key: field.key }))
  worksheet.columns = columns

  const writableXlsx = new Writable({
    objectMode: true,
    write (line, _, next) {
      worksheet.addRow(line).commit()
      next()
    },
    final (callback) {
      worksheet.commit()
      workbook.commit()
        .then(() => callback())
        .catch(callback)
    }
  })

  return [writableXlsx]
}

const maxRetries = 3
/**
 * @param {string} url
 * @param {import('axios').AxiosInstance} axios
 * @param {any} log
 * @returns {Promise<{results: any[], next?: string, total: number}>}
 */
async function fetchData (url, axios, log) {
  let retries = 0
  let error
  while (retries < maxRetries) {
    try {
      const response = await axios(url)
      const data = response.data
      return data
    } catch (_error) {
      error = _error
      await log.warning(`La requête a échoué. Tentatives restantes : ${maxRetries - retries}`)
      await new Promise(resolve => setTimeout(resolve, 1000))
      retries++
    }
  }
  await log.error('Échec de la requête après plusieurs tentatives. Abandon.')
  throw error
}

const dataSize = 10000
/**
 * @param {any} processingConfig
 * @param {import('axios').AxiosInstance} axios
 * @param {any} log
 * @param {import('node:stream').Writable[]} writeStreams
 * @returns {Promise<void>}
 */
async function fetchAndWriteData (processingConfig, axios, log, writeStreams) {
  const urlObj = new URL(processingConfig.dataset.href + '/lines')
  urlObj.searchParams.set('size', dataSize.toString())
  urlObj.searchParams.set('select', processingConfig.fields.map((/** @type {{key: string}} */field) => field.key).join(','))
  if (processingConfig.filter && processingConfig.filter.field && processingConfig.filter.value) {
    urlObj.searchParams.set('qs', `${processingConfig.filter.field}:${processingConfig.filter.value}`)
  }

  /** @type {string | undefined} */
  let url = urlObj.href

  await log.task('Téléchargement des données')
  let count = 0

  while (url) {
    const data = await fetchData(url, axios, log)
    url = data.next
    for (const line of data.results) {
      delete line._score
      for (const field of processingConfig.fields) {
        if (line[field.key] === null) line[field.key] = undefined
      }
      for (const writeStream of writeStreams) {
        // writing to the stream without piping but while still respecting backpressure
        const keepWriting = writeStream.write(line)
        if (!keepWriting) await new Promise(resolve => writeStream.once('drain', resolve))
      }
    }

    count += data.results.length
    await log.progress('Téléchargement des données', count, data.total)
  }
  for (const writeStream of writeStreams) writeStream.end()
}

/**
 * @param {string} filePath
 * @param {any} processingConfig
 * @param {import('axios').AxiosInstance} axios
 * @param {any} log
 */
async function upload (filePath, processingConfig, axios, log) {
  const filename = path.parse(filePath).base
  const formData = new FormData()
  formData.append('attachment', fs.createReadStream(filePath), { filename })
  const getLengthAsync = util.promisify(formData.getLength).bind(formData)
  const contentLength = await getLengthAsync()

  const { formatBytes } = await import('@data-fair/lib/format/bytes.js')
  await log.info(`Chargement de la pièce jointe ${filename}, taille : (${formatBytes(contentLength)})`)

  const response = await axios({
    method: 'post',
    url: `${processingConfig.dataset.href}/metadata-attachments`,
    data: formData,
    maxContentLength: Infinity,
    maxBodyLength: Infinity,
    headers: { ...formData.getHeaders(), 'content-length': contentLength }
  })
  await log.info('Chargement de la pièce jointe terminé')
  await log.info('Mise à jour des métadonnées')

  const attachments = (await axios(processingConfig.dataset.href + '?select=attachments')).data.attachments || []
  const idx = attachments.findIndex((/** @type {{name: string}} */a) => a.name === filename)
  if (idx >= 0) attachments.splice(idx, 1)
  await axios({
    method: 'patch',
    url: processingConfig.dataset.href,
    data: {
      attachments: [...attachments,
        {
          ...response.data,
          type: 'file',
          title: processingConfig.label
        }
      ]
    }
  })
}

/**
 * @param {{processingConfig: any, tmpDir: string, axios: import('axios').AxiosInstance, log: any}} processingContext
 * @returns
 */
exports.run = async ({ processingConfig, tmpDir, axios, log }) => {
  await log.step('Récupération des données')

  /** @type {import('node:stream').Writable[][]} */
  const streamPipelines = []
  /** @type {string[]} */
  const filePaths = []
  if (processingConfig.format.includes('csv')) {
    const filePathCsv = path.join(tmpDir, processingConfig.filename + '.csv')
    streamPipelines.push(getCSVStreamPipeline(filePathCsv, processingConfig))
    filePaths.push(filePathCsv)
  }
  if (processingConfig.format.includes('parquet')) {
    const filePathParquet = path.join(tmpDir, processingConfig.filename + '.parquet')
    streamPipelines.push(getParquetStreamPipeline(filePathParquet, processingConfig))
    filePaths.push(filePathParquet)
  }
  if (processingConfig.format.includes('xlsx')) {
    const filePathXlsx = path.join(tmpDir, processingConfig.filename + '.xlsx')
    streamPipelines.push(getXlsxStreamPipeline(filePathXlsx, processingConfig))
    filePaths.push(filePathXlsx)
  }

  const promises = streamPipelines.map(streams => {
    return streams.length > 1 ? pipeline(streams) : finished(streams[0])
  })
  promises.push(fetchAndWriteData(processingConfig, axios, log, streamPipelines.map(streams => streams[0])))
  await Promise.all(promises)

  if (stopped) return
  await log.step('Chargement des pièces jointes')
  for (const filePath of filePaths) {
    await upload(filePath, processingConfig, axios, log)
  }
}

exports.stop = () => {
  stopped = true
}
