import fs from 'node:fs'
import { strict as assert } from 'node:assert'
import { after, before, describe, it } from 'node:test'
import config from 'config'
import nock from 'nock'
import testUtils from '@data-fair/lib-processing-dev/tests-utils.js'
import * as exportFilePlugin from '../index.ts'

const DATA_FAIR = 'http://test.data-fair.local'
const DATASET_ID = 'test-stop-dataset'
const DATASET_HREF = `${DATA_FAIR}/api/v1/datasets/${DATASET_ID}`
const TMP = 'data/tmp-stop'

describe('export-file — graceful stop', () => {
  before(() => {
    fs.mkdirSync(TMP, { recursive: true })
  })

  after(() => {
    nock.cleanAll()
    fs.rmSync(TMP, { recursive: true, force: true })
  })

  it('aborts run cleanly when stop() is called mid-fetch', async () => {
    const dataset = {
      id: DATASET_ID,
      title: 'Stop test',
      schema: [{ key: 'name', type: 'string' }],
      attachments: [],
      bbox: null
    }
    const linesPage1 = {
      total: 10,
      next: `${DATASET_HREF}/lines?page=2`,
      results: [{ name: 'a' }]
    }

    let attachmentCalled = false

    nock(DATA_FAIR)
      .get(`/api/v1/datasets/${DATASET_ID}`).reply(200, dataset)
      .get(`/api/v1/datasets/${DATASET_ID}/lines`).query(true).reply(200, linesPage1)
      .get(`/api/v1/datasets/${DATASET_ID}/lines`).query(true).delay(500).reply(200, {
        total: 10, results: [{ name: 'b' }]
      })
      .post(`/api/v1/datasets/${DATASET_ID}/metadata-attachments`).reply(200, () => {
        attachmentCalled = true
        return { name: 'x', size: 1 }
      })

    const processingConfig: any = {
      dataset: { id: DATASET_ID, href: DATASET_HREF, title: dataset.title },
      fields: [{ key: 'name', type: 'string' }],
      format: ['csv'],
      filename: 'export-stop',
      label: 'Export'
    }

    const context = testUtils.context({
      tmpDir: TMP,
      processingConfig
      // @ts-ignore ProcessingTestConfig should be optional in lib-processing-dev
    }, config, false)

    setTimeout(() => { exportFilePlugin.stop().catch(() => {}) }, 200)

    await exportFilePlugin.run(context)

    assert.equal(attachmentCalled, false, 'no attachment uploaded after stop')
  })
})
