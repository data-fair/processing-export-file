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
})
