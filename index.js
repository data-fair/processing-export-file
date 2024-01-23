const fs = require('fs-extra')
const path = require('path')
const FormData = require('form-data')
const util = require('util')
const Stream = require('stream')
const pump = require('pump')
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
  const readableStream = new Stream.Readable({ objectMode: true })
  readableStream._read = () => {}
  const filePath = path.join(tmpDir, processingConfig.filename + '.csv')
  const writeStream = fs.createWriteStream(filePath, { flags: 'w' })

  pump(
    readableStream,
    csv.stringify({ header: true, quoted_string: true }),
    writeStream
  )

  let next = processingConfig.dataset.href + '/lines?size=1000&select=' + processingConfig.fields.join(',')
  if (processingConfig.filter && processingConfig.filter.field && processingConfig.filter.value) next += `&qs=${processingConfig.filter.field}:${processingConfig.filter.value}`
  do {
    const response = (await axios(next)).data
    await log.info(response.results.length + ' lignes récupérées')
    for (const result of response.results) {
      delete result._score
      readableStream.push(result)
    }
    next = response.next
  } while (next)
  readableStream.push()

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

  const attachments = (await axios(processingConfig.dataset.href + '?select=attachments')).data.attachments
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
