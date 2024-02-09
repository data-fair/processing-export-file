const fs = require('fs-extra')
const path = require('path')
const FormData = require('form-data')
const util = require('util')
const Stream = require('stream')
const pump = require('util').promisify(require('pump'))
const { ParquetSchema, ParquetTransformer } = require('@dsnp/parquetjs')
const csv = require('csv')
const Excel = require('exceljs')

function displayBytes (aSize) {
  aSize = Math.abs(parseInt(aSize, 10))
  if (aSize === 0) return '0 octets'
  const def = [[1, 'octets'], [1000, 'ko'], [1000 * 1000, 'Mo'], [1000 * 1000 * 1000, 'Go'], [1000 * 1000 * 1000 * 1000, 'To'], [1000 * 1000 * 1000 * 1000 * 1000, 'Po']]
  for (let i = 0; i < def.length; i++) {
    if (aSize < def[i][0]) return (aSize / def[i - 1][0]).toLocaleString() + ' ' + def[i - 1][1]
  }
}

// main execution method
exports.run = async ({ processingConfig, tmpDir, axios, log }) => {
  async function * dataPages (url) {
    async function * makeRequest (_url, retries = 3) {
      try {
        const response = await axios(_url)
        const data = response.data
        await log.info(data.results.length + ' lignes récupérées')
        yield data.results
        if (data.next) {
          yield * makeRequest(data.next)
        }
      } catch (error) {
        if (retries > 0) {
          await log.warning(`La requête a échoué. Tentatives restantes : ${retries}`)
          await new Promise(resolve => setTimeout(resolve, 1000))
          yield * makeRequest(_url, retries - 1)
        } else {
          await log.error('Échec de la requête après plusieurs tentatives. Abandon.')
          throw error
        }
      }
    }
    yield * makeRequest(url)
  }

  async function * depaginate (url) {
    for await (const page of dataPages(url)) {
      for (const line of page) {
        delete line._score
        for (const field of processingConfig.fields) {
          if (line[field.key] === null) line[field.key] = undefined
        }
        yield line
      }
    }
  }

  async function upload (filePath) {
    const filename = path.parse(filePath).base
    const formData = new FormData()
    formData.append('attachment', fs.createReadStream(filePath), { filename })
    formData.getLength = util.promisify(formData.getLength)
    const contentLength = await formData.getLength()
    await log.info(`Chargement de la pièce jointe ${filename}, taille : (${displayBytes(contentLength)})`)

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
    const idx = attachments.findIndex(a => a.name === filename)
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

  let url = processingConfig.dataset.href + '/lines?size=10000&select=' + processingConfig.fields.map(field => field.key).join(',')
  if (processingConfig.filter && processingConfig.filter.field && processingConfig.filter.value) url += `&qs=${processingConfig.filter.field}:${processingConfig.filter.value}`

  const readableStream = Stream.Readable.from(depaginate(url), { objectMode: true })
  await fs.ensureDir(tmpDir)
  const promises = []

  if (processingConfig.format.includes('csv')) {
    const csvPromise = (async () => {
      const filePathCsv = path.join(tmpDir, processingConfig.filename + '.csv')
      const writeStreamCsv = fs.createWriteStream(filePathCsv, { flags: 'w' })

      await pump(
        readableStream,
        csv.stringify({ header: true, quoted_string: true, columns: processingConfig.fields.map(field => field.key), cast: { boolean: value => value ? '1' : '0' } }),
        writeStreamCsv
      )

      return filePathCsv
    })()

    promises.push(csvPromise)
  }

  if (processingConfig.format.includes('parquet')) {
    const parquetPromise = (async () => {
      const filePathParquet = path.join(tmpDir, processingConfig.filename + '.parquet')
      const writeStreamParquet = fs.createWriteStream(filePathParquet, { flags: 'w' })

      const typeConversion = {
        integer: 'INT64',
        number: 'FLOAT',
        string: 'UTF8',
        boolean: 'BOOLEAN'
      }

      const schemaDefinition = {}
      processingConfig.fields.forEach(column => {
        schemaDefinition[column.key] = { type: typeConversion[column.type], optional: true }
      })
      const schema = new ParquetSchema(schemaDefinition)

      await pump(
        readableStream,
        new ParquetTransformer(schema),
        writeStreamParquet
      )

      return filePathParquet
    })()

    promises.push(parquetPromise)
  }

  if (processingConfig.format.includes('xlsx')) {
    const xlsxPromise = (async () => {
      const filePathXlsx = path.join(tmpDir, processingConfig.filename + '.xlsx')
      const writeStreamXlsx = fs.createWriteStream(filePathXlsx, { flags: 'w' })

      const workbook = new Excel.stream.xlsx.WorkbookWriter({ stream: writeStreamXlsx })
      const worksheet = workbook.addWorksheet(processingConfig.label)

      const columns = processingConfig.fields.map(field => ({ header: field.key, key: field.key }))
      worksheet.columns = columns

      await pump(
        readableStream,
        new Stream.Transform({
          objectMode: true,
          transform (line, _, next) {
            worksheet.addRow(line)
            next()
          }
        })
      )

      worksheet.commit()
      await workbook.commit()
      return filePathXlsx
    })()

    promises.push(xlsxPromise)
  }

  const filePaths = await Promise.all(promises)
  for (const filePath of filePaths) {
    await upload(filePath)
  }
}
