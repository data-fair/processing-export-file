import fs from 'node:fs'
import path from 'node:path'
import { spawnSync } from 'node:child_process'
import { strict as assert } from 'node:assert'
import { after, before, describe, it } from 'node:test'
import config from 'config'
import nock from 'nock'
import testUtils from '@data-fair/lib-processing-dev/tests-utils.js'
import * as exportFilePlugin from '../index.ts'

const DATA_FAIR = 'http://test.data-fair.local'
const DATASET_ID = 'test-geo-dataset'
const DATASET_HREF = `${DATA_FAIR}/api/v1/datasets/${DATASET_ID}`
const TMP = 'data/tmp-run-geo'

const hasBinary = (cmd: string) => spawnSync('which', [cmd], { stdio: 'ignore' }).status === 0
const hasOgr2ogr = hasBinary('ogr2ogr')
const skipReason = !hasOgr2ogr ? 'ogr2ogr not installed' : undefined

describe('export-file — geo formats with nock', { skip: skipReason }, () => {
  before(() => {
    fs.mkdirSync(TMP, { recursive: true })
  })

  after(() => {
    nock.cleanAll()
    fs.rmSync(TMP, { recursive: true, force: true })
  })

  it('exports geojson + pmtiles + shp + gpkg from a geo dataset', async () => {
    const dataset = {
      id: DATASET_ID,
      title: 'Geo test dataset',
      schema: [
        { key: 'name', type: 'string' },
        { key: 'lat', type: 'number', 'x-concept': { id: 'latitude' } },
        { key: 'lng', type: 'number', 'x-concept': { id: 'longitude' } }
      ],
      attachments: [],
      bbox: [0, 0, 10, 10]
    }
    const lines = {
      total: 2,
      results: [
        { name: 'paris', lat: 48.85, lng: 2.35 },
        { name: 'lyon', lat: 45.76, lng: 4.83 }
      ]
    }

    const scope = nock(DATA_FAIR)
      .get(`/api/v1/datasets/${DATASET_ID}`).reply(200, dataset)
      .get(`/api/v1/datasets/${DATASET_ID}/lines`).query(true).reply(200, lines)
      .post(`/api/v1/datasets/${DATASET_ID}/metadata-attachments`).times(4)
      .reply(200, { name: 'export.geojson', size: 100 })
      .patch(`/api/v1/datasets/${DATASET_ID}`).times(4).reply(200, {})

    const processingConfig: any = {
      dataset: { id: DATASET_ID, href: DATASET_HREF, title: dataset.title },
      fields: [{ key: 'name', type: 'string' }, { key: 'lat', type: 'number' }, { key: 'lng', type: 'number' }],
      format: ['geojson', 'pmtiles', 'shp', 'gpkg'],
      filename: 'export',
      label: 'Export'
    }

    const context = testUtils.context({
      tmpDir: TMP,
      processingConfig
      // @ts-ignore ProcessingTestConfig should be optional in lib-processing-dev
    }, config, false)

    await exportFilePlugin.run(context)

    assert.ok(fs.existsSync(path.join(TMP, 'export.geojson')))
    assert.ok(fs.existsSync(path.join(TMP, 'export.pmtiles')))
    assert.ok(fs.existsSync(path.join(TMP, 'export.zip')))
    assert.ok(fs.existsSync(path.join(TMP, 'export.gpkg')))
    assert.ok(scope.isDone())
  })
})
