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
    async function * makeRequest (_url) {
      const response = (await axios(_url)).data
      await log.info(response.results.length + ' lignes récupérées')
      yield response.results
      if (response.next) {
        yield * makeRequest(response.next)
      }
    }
    yield * makeRequest(url)
  }

  async function * depaginate () {
    const url = processingConfig.dataset.href + '/lines?size=10000&select=' + processingConfig.fields.join(',')
    const pages = dataPages(url)

    for await (const page of pages) {
      for (const line of page) {
        yield line
      }
    }
  }

  const readableStream = Stream.Readable.from(depaginate(), { objectMode: true })
  const filePath = path.join(tmpDir, processingConfig.filename + '.csv')
  const writeStream = fs.createWriteStream(filePath, { flags: 'w' })

  await pump(
    readableStream,
    csv.stringify({ header: true, quoted_string: true }),
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
