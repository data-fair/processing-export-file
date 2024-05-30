const fs = require('node:fs')
const path = require('node:path')
const { Writable } = require('node:stream')
const { pipeline, finished } = require('node:stream/promises')
const util = require('node:util')
const FormData = require('form-data')
const exec = require('./lib/exec')

let stopped = false

/**
 * @param {string} filePath
 * @param {any} processingConfig
 * @returns {import('node:stream').Writable[]}
 */
function getCSVStreamPipeline (filePath, processingConfig) {
  const csv = require('csv-stringify')
  const params = {
    header: true,
    quoted_string: true,
    cast: { boolean: value => value ? '1' : '0' }
  }
  if (processingConfig.fields.length) params.columns = processingConfig.fields.map((/** @type {{key: string}} */field) => field.key)
  return [
    csv.stringify(params),
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
  if (processingConfig.fields.length) {
    urlObj.searchParams.set('select', processingConfig.fields.map((/** @type {{key: string}} */field) => field.key).join(','))
  }
  if (processingConfig.filter && processingConfig.filter.field && processingConfig.filter.value) {
    urlObj.searchParams.set('qs', `${processingConfig.filter.field}:${processingConfig.filter.value}`)
  }

  /** @type {string | undefined} */
  let url = urlObj.href

  await log.task('Téléchargement des données')
  let count = 0

  while (url) {
    if (stopped) return
    const { data } = await axios(url)
    url = data.next
    for (const line of data.results) {
      delete line._score
      for (const field of processingConfig.fields) {
        if (line[field.key] === null) line[field.key] = undefined
      }
      if (line._geopoint) {
        const [lat, lon] = line._geopoint.split(',')
        line.latitude = lat
        line.longitude = lon
      }
      if (line['_geoshape.coordinates']) {
        line._geoshape = JSON.stringify({
          coordinates: line['_geoshape.coordinates'],
          type: line['_geoshape.type']
        })
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
async function upload (filePath, dataset, processingConfig, axios, log) {
  const filename = path.parse(filePath).base
  const formData = new FormData()
  formData.append('attachment', fs.createReadStream(filePath), { filename })
  const getLengthAsync = util.promisify(formData.getLength).bind(formData)
  const contentLength = await getLengthAsync()

  const { formatBytes } = await import('@data-fair/lib/format/bytes.js')
  const task = `Chargement de la pièce jointe ${filename} (${formatBytes(contentLength)})`
  await log.task(task)

  const response = await axios({
    method: 'post',
    url: `${processingConfig.dataset.href}/metadata-attachments`,
    data: formData,
    maxContentLength: Infinity,
    maxBodyLength: Infinity,
    headers: { ...formData.getHeaders(), 'content-length': contentLength },
    onUploadProgress: progressEvent => {
      log.progress(task, progressEvent.loaded, progressEvent.total)
    }
  })

  await log.info('Mise à jour des métadonnées')

  const attachments = dataset.attachments || []
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
  if (processingConfig.format.includes('csv') || processingConfig.format.includes('pmtiles')) {
    const filePathCsv = path.join(tmpDir, processingConfig.filename + '.csv')
    streamPipelines.push(getCSVStreamPipeline(filePathCsv, processingConfig))
    if (processingConfig.format.includes('csv')) filePaths.push(filePathCsv)
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
  const dataset = (await axios(processingConfig.dataset.href)).data

  if (processingConfig.format.includes('pmtiles') || processingConfig.format.includes('shz') || processingConfig.format.includes('gpkg') || processingConfig.format.includes('geojson')) {
    if (!dataset.bbox) {
      await log.error('Le jeu de données n\'est pas géographique et ne peut pas être converti')
      return
    }
    const filePathCsv = path.join(tmpDir, processingConfig.filename + '.csv')
    const filePathGeojson = path.join(tmpDir, processingConfig.filename + '.geojson')
    if (processingConfig.format.includes('geojson')) {
      filePaths.push(filePathGeojson)
    }
    const ogr2ogrOptions = ['-f', 'GEOJSON', filePathGeojson, filePathCsv, '-a_srs', 'EPSG:4326']
    const geomField = dataset.schema.find(f => (f['x-concept'] && f['x-concept'].id === 'geometry') || f.key === '_geoshape')
    const latField = dataset.schema.find(f => (f['x-concept'] && f['x-concept'].id === 'latitude') || f.key === 'latitude')
    const lonField = dataset.schema.find(f => (f['x-concept'] && f['x-concept'].id === 'longitude') || f.key === 'longitude')
    if (geomField) {
      ogr2ogrOptions.push('-oo')
      ogr2ogrOptions.push(`GEOM_POSSIBLE_NAMES=${geomField.key}`)
    } else if (latField && lonField) {
      ogr2ogrOptions.push('-oo')
      ogr2ogrOptions.push(`X_POSSIBLE_NAMES=${lonField.key}`)
      ogr2ogrOptions.push('-oo')
      ogr2ogrOptions.push(`Y_POSSIBLE_NAMES=${latField.key}`)
    } else {
      await log.error('Les concepts nécessaires n\'ont pas été trouvés')
      return
    }
    await exec('ogr2ogr', ogr2ogrOptions)
    if (processingConfig.format.includes('pmtiles')) {
      const filePathPmtiles = path.join(tmpDir, processingConfig.filename + '.pmtiles')
      await exec('tippecanoe', ['-zg', '--projection=EPSG:4326', '--force', '--drop-densest-as-needed', '-o', filePathPmtiles, '-l', 'default', filePathGeojson])
      filePaths.push(filePathPmtiles)
    }
    if (processingConfig.format.includes('shz')) {
      const filePathShz = path.join(tmpDir, processingConfig.filename + '.shz')
      await exec('ogr2ogr', ['-f', 'ESRI Shapefile', '-skipfailures', filePathShz, filePathGeojson])
      filePaths.push(filePathShz)
    }
    if (processingConfig.format.includes('gpkg')) {
      const filePathGpkg = path.join(tmpDir, processingConfig.filename + '.gpkg')
      await exec('ogr2ogr', ['-f', 'GPKG', '-skipfailures', filePathGpkg, filePathGeojson])
      filePaths.push(filePathGpkg)
    }
  }

  if (stopped) return
  await log.step('Chargement des pièces jointes')
  for (const filePath of filePaths) {
    if (stopped) return
    await upload(filePath, dataset, processingConfig, axios, log)
  }
}

exports.stop = async () => {
  stopped = true
}
