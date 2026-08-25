import fs from 'node:fs'
import path from 'node:path'
import { strict as assert } from 'node:assert'
import { after, before, describe, it } from 'node:test'
import config from 'config'
import nock from 'nock'
import testUtils from '@data-fair/lib-processing-dev/tests-utils.js'
import * as exportFilePlugin from '../index.ts'

const DATA_FAIR = 'http://test.data-fair.local'
const DATASET_ID = 'test-dataset'
const DATASET_HREF = `${DATA_FAIR}/api/v1/datasets/${DATASET_ID}`
const TMP = 'data/tmp-run'

describe('export-file — run with nock', () => {
  before(() => {
    fs.mkdirSync(TMP, { recursive: true })
  })

  after(() => {
    nock.cleanAll()
    fs.rmSync(TMP, { recursive: true, force: true })
  })

  it('exports csv + xlsx + parquet from a paginated dataset', async () => {
    const dataset = {
      id: DATASET_ID,
      title: 'Test dataset',
      schema: [
        { key: 'name', type: 'string' },
        { key: 'count', type: 'integer' }
      ],
      attachments: [],
      bbox: null
    }

    const linesPage1 = {
      total: 3,
      next: `${DATASET_HREF}/lines?page=2`,
      results: [
        { name: 'alice', count: 1 },
        { name: 'bob', count: 2 }
      ]
    }
    const linesPage2 = {
      total: 3,
      results: [{ name: 'carol', count: 3 }]
    }

    const scope = nock(DATA_FAIR)
      .get(`/api/v1/datasets/${DATASET_ID}`).reply(200, dataset)
      .get(`/api/v1/datasets/${DATASET_ID}/lines`).query((q: any) => q.size === '0')
      .reply(200, { total: 3, results: [] })
      .get(`/api/v1/datasets/${DATASET_ID}/lines`).query(true).reply(200, linesPage1)
      .get(`/api/v1/datasets/${DATASET_ID}/lines`).query(true).reply(200, linesPage2)
      .post(`/api/v1/datasets/${DATASET_ID}/metadata-attachments`).times(3)
      .reply(200, (_uri, _body) => ({ name: 'export.csv', size: 100 }))
      .patch(`/api/v1/datasets/${DATASET_ID}`).times(3).reply(200, {})

    const processingConfig: any = {
      dataset: { id: DATASET_ID, href: DATASET_HREF, title: dataset.title },
      fields: [{ key: 'name', type: 'string' }, { key: 'count', type: 'integer' }],
      format: ['csv', 'xlsx', 'parquet'],
      filename: 'export',
      label: 'Export'
    }

    const context = testUtils.context({
      tmpDir: TMP,
      processingConfig
      // @ts-ignore ProcessingTestConfig should be optional in lib-processing-dev
    }, config, false)

    await exportFilePlugin.run(context)

    assert.ok(fs.existsSync(path.join(TMP, 'export.csv')), 'csv file was produced')
    assert.ok(fs.existsSync(path.join(TMP, 'export.xlsx')), 'xlsx file was produced')
    assert.ok(fs.existsSync(path.join(TMP, 'export.parquet')), 'parquet file was produced')

    const csvContent = fs.readFileSync(path.join(TMP, 'export.csv'), 'utf8')
    assert.match(csvContent, /"?name"?,"?count"?/, 'csv has header')
    assert.match(csvContent, /alice/)
    assert.match(csvContent, /carol/)

    assert.ok(scope.isDone(), 'all nock scopes consumed')
  })

  it('skips the xlsx export with a warning when the dataset exceeds the Excel row limit', async () => {
    const datasetId = 'test-huge-dataset'
    const datasetHref = `${DATA_FAIR}/api/v1/datasets/${datasetId}`
    const dataset = {
      id: datasetId,
      title: 'Huge dataset',
      schema: [{ key: 'name', type: 'string' }],
      attachments: [],
      bbox: null
    }

    const scope = nock(DATA_FAIR)
      .get(`/api/v1/datasets/${datasetId}`).reply(200, dataset)
      .get(`/api/v1/datasets/${datasetId}/lines`).query((q: any) => q.size === '0')
      .reply(200, { total: 2000000, results: [] })
      .get(`/api/v1/datasets/${datasetId}/lines`).query(true)
      .reply(200, { total: 2000000, results: [{ name: 'alice' }] })
      .post(`/api/v1/datasets/${datasetId}/metadata-attachments`)
      .reply(200, { name: 'export-huge.csv', size: 100 })
      .patch(`/api/v1/datasets/${datasetId}`).reply(200, {})

    const processingConfig: any = {
      dataset: { id: datasetId, href: datasetHref, title: dataset.title },
      fields: [{ key: 'name', type: 'string' }],
      format: ['csv', 'xlsx'],
      filename: 'export-huge',
      label: 'Export'
    }

    const context = testUtils.context({
      tmpDir: TMP,
      processingConfig
      // @ts-ignore ProcessingTestConfig should be optional in lib-processing-dev
    }, config, false)

    const warnings: string[] = []
    const originalWarning = context.log.warning
    context.log.warning = async (msg: string, extra?: any) => {
      warnings.push(msg)
      return originalWarning(msg, extra)
    }

    await exportFilePlugin.run(context)

    assert.equal(warnings.length, 1, 'a single warning was emitted')
    assert.match(warnings[0], /1,048,575 rows/)
    assert.match(warnings[0], /2,000,000 rows/)
    assert.ok(!fs.existsSync(path.join(TMP, 'export-huge.xlsx')), 'xlsx file was not produced')
    assert.ok(fs.existsSync(path.join(TMP, 'export-huge.csv')), 'csv file was still produced')
    assert.ok(scope.isDone(), 'all nock scopes consumed')
  })
})
