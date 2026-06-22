import fs from 'node:fs'
import fsp from 'node:fs/promises'
import path from 'node:path'
import { pipeline, finished } from 'node:stream/promises'
import type { Writable } from 'node:stream'
import archiver from 'archiver'
import type { RunFunction, ProcessingContext } from '@data-fair/lib-common-types/processings.js'
import type { ProcessingConfig } from '#types/processingConfig/index.ts'
import type { Field, GeomField } from './transforms.ts'
import { fetchAndWriteData } from './fetch.ts'
import { uploadAttachment } from './upload.ts'
import { getCsvPipeline, getParquetPipeline, getXlsxPipeline } from './pipelines.ts'
import { buildVrt } from './vrt.ts'
import { runCommand } from './spawn-process.ts'

let shouldBeStopped = false

export const run: RunFunction<ProcessingConfig> = async (context) => {
  shouldBeStopped = false
  const { processingConfig, tmpDir, axios, log } = context as ProcessingContext<ProcessingConfig>
  const cfg: any = processingConfig

  await log.step('Fetching data')

  const dataset = (await axios(cfg.dataset.href)).data
  const geomField: GeomField | undefined = dataset.schema.find((f: any) => f['x-concept']?.id === 'geometry')
  const latField: Field | undefined = dataset.schema.find((f: any) => f['x-concept']?.id === 'latitude')
  const lonField: Field | undefined = dataset.schema.find((f: any) => f['x-concept']?.id === 'longitude')
  const latLonField: Field | undefined = dataset.schema.find((f: any) => f['x-concept']?.id === 'latLon')

  const fields: Field[] = Array.isArray(cfg.fields) && cfg.fields.length
    ? [...cfg.fields]
    : dataset.schema.filter((f: any) => !f['x-calculated'])

  const formats: string[] = cfg.format ?? ['csv']
  const needsGeo = formats.some(f => ['pmtiles', 'shp', 'gpkg', 'geojson'].includes(f))
  if (needsGeo) {
    if (latField && lonField) {
      if (!fields.find(f => f.key === latField.key)) fields.push(latField)
      if (!fields.find(f => f.key === lonField.key)) fields.push(lonField)
    } else if (geomField && !fields.find(f => f.key === geomField.key)) {
      fields.push(geomField as Field)
    } else if (latLonField && !fields.find(f => f.key === latLonField.key)) {
      fields.push(latLonField)
    }
  }

  const streamPipelines: Writable[][] = []
  const filePaths: string[] = []
  const filename: string = cfg.filename ?? 'export'
  const label: string = cfg.label ?? 'Export'

  if (formats.includes('csv') || needsGeo) {
    const p = path.join(tmpDir, filename + '.csv')
    streamPipelines.push(getCsvPipeline(p, fields))
    if (formats.includes('csv')) filePaths.push(p)
  }
  if (formats.includes('parquet')) {
    const p = path.join(tmpDir, filename + '.parquet')
    streamPipelines.push(getParquetPipeline(p, fields))
    filePaths.push(p)
  }
  if (formats.includes('xlsx')) {
    const p = path.join(tmpDir, filename + '.xlsx')
    streamPipelines.push(getXlsxPipeline(p, fields, label))
    filePaths.push(p)
  }

  const pipelinePromises = streamPipelines.map(streams =>
    streams.length > 1 ? pipeline(streams) : finished(streams[0])
  )
  pipelinePromises.push(fetchAndWriteData({
    datasetHref: cfg.dataset.href,
    fields,
    filters: cfg.filters,
    geomField,
    axios,
    log,
    writeStreams: streamPipelines.map(streams => streams[0]),
    isStopped: () => shouldBeStopped
  }))
  await Promise.all(pipelinePromises)

  if (needsGeo) {
    if (!dataset.bbox) {
      await log.error('The dataset is not geographic and cannot be converted to a geographic format')
      return
    }
    if (!geomField && !latLonField && (!latField || !lonField)) {
      await log.error('Required geographic concepts (geometry, latitude/longitude, or latLon) were not found')
      return
    }

    await log.step('Generating files')
    const csvPath = path.join(tmpDir, filename + '.csv')
    const geojsonPath = path.join(tmpDir, filename + '.geojson')
    if (formats.includes('geojson')) filePaths.push(geojsonPath)

    const vrtPath = path.join(tmpDir, filename + '.vrt')
    await fsp.writeFile(vrtPath, buildVrt({ filename, csvPath, fields, geomField, latField, lonField, latLonField }))
    await runCommand('ogr2ogr', ['-f', 'GEOJSON', geojsonPath, vrtPath])

    if (formats.includes('pmtiles')) {
      await log.info('Generating pmtiles file')
      const p = path.join(tmpDir, filename + '.pmtiles')
      await runCommand('ogr2ogr', ['-f', 'PMTiles', p, geojsonPath, '-nln', 'default'])
      filePaths.push(p)
    }
    if (formats.includes('shp')) {
      await log.info('Generating shp file')
      const shpDir = path.join(tmpDir, filename + '_shp')
      const p = path.join(tmpDir, filename + '-shp.zip')
      await runCommand('ogr2ogr', ['-f', 'ESRI Shapefile', '-skipfailures', shpDir, geojsonPath])
      await new Promise<void>((resolve, reject) => {
        const output = fs.createWriteStream(p)
        const archive = archiver('zip')
        output.on('close', resolve)
        output.on('error', reject)
        archive.on('error', reject)
        archive.pipe(output)
        archive.directory(shpDir, false)
        archive.finalize().catch(reject)
      })
      filePaths.push(p)
    }
    if (formats.includes('gpkg')) {
      await log.info('Generating gpkg file')
      const p = path.join(tmpDir, filename + '.gpkg')
      await runCommand('ogr2ogr', ['-f', 'GPKG', '-skipfailures', p, geojsonPath])
      filePaths.push(p)
    }
  }

  if (shouldBeStopped) return
  await log.step('Uploading attachments')
  for (const filePath of filePaths) {
    if (shouldBeStopped) return
    await uploadAttachment({ filePath, dataset, datasetHref: cfg.dataset.href, label, axios, log })
  }
}

export const stop: () => Promise<void> = async () => { shouldBeStopped = true }
