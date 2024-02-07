const fs = require('fs-extra')
const path = require('path')
const FormData = require('form-data')
const util = require('util')
const Stream = require('stream')
const pump = require('util').promisify(require('pump'))
const csv = require('csv')

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
          await log.warn(`La requête a échoué. Tentatives restantes : ${retries}`)
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
          if (line[field] == null) line[field] = undefined
        }
        yield line
      }
    }
  }

  let url = processingConfig.dataset.href + '/lines?size=10000&select=' + processingConfig.fields.join(',')
  if (processingConfig.filter && processingConfig.filter.field && processingConfig.filter.value) url += `&qs=${processingConfig.filter.field}:${processingConfig.filter.value}`

  const readableStream = Stream.Readable.from(depaginate(url), { objectMode: true })
  await fs.ensureDir(tmpDir)
  const filePath = path.join(tmpDir, processingConfig.filename + '.csv')
  const writeStream = fs.createWriteStream(filePath, { flags: 'w' })

  await pump(
    readableStream,
    csv.stringify({ header: true, quoted_string: true, columns: processingConfig.fields }),
    writeStream
  )

  const filename = path.parse(filePath).base
  const formData = new FormData()
  formData.append('attachment', fs.createReadStream(filePath), { filename })
  formData.getLength = util.promisify(formData.getLength)
  const contentLength = await formData.getLength()
  await log.info(`Chargement de la pièce jointe, taille : (${displayBytes(contentLength)})`)

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
