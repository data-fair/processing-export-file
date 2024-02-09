process.env.NODE_ENV = 'test'
const config = require('config')
const assert = require('assert').strict
const exportFile = require('../')

describe('Hello world processing', () => {
  it('should expose a plugin config schema for super admins', async () => {
    const schema = require('../plugin-config-schema.json')
    assert.ok(schema)
  })
  it('should expose a processing config schema for users', async () => {
    const schema = require('../processing-config-schema.json')
    assert.equal(schema.type, 'object')
  })

  it('should run a task', async function () {
    this.timeout(1000000)

    const testsUtils = await import('@data-fair/lib/processings/tests-utils.js')
    const context = testsUtils.context({
      pluginConfig: {},
      processingConfig: {
        dataset: {
          title: 'RNIC test',
          id: 'prnt0u0rhqjlkrd1wrnyozeu', // RNIC issu du test de transform-csv
          href: 'https://staging-koumoul.com/data-fair/api/v1/datasets/prnt0u0rhqjlkrd1wrnyozeu'
        },
        format: ['csv', 'parquet', 'xlsx'],
        fields: [
          {
            key: 'nom_copropriete',
            type: 'string'
          },
          {
            key: 'latitude',
            type: 'number'
          },
          {
            key: 'longitude',
            type: 'number'
          },
          {
            key: 'nb_lots_habitation',
            type: 'integer'
          },
          {
            key: 'syndicat_cooperatif',
            type: 'boolean'
          },
          {
            key: 'parcelles',
            type: 'string'
          }
        ],
        label: 'Mon export test',
        filename: 'my-export'
      },
      tmpDir: 'data/'
    }, config, false)
    await exportFile.run(context)
  })
})
