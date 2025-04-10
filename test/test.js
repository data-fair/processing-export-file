process.env.NODE_ENV = 'test'

/** @type {any} */
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
    // @ts-ignore
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
    }, config, false, false)
    await exportFile.run(context)
  })

  it('should export a pm tiles', async function () {
    // @ts-ignore
    this.timeout(1000000)

    const testsUtils = await import('@data-fair/lib/processings/tests-utils.js')
    const context = testsUtils.context({
      pluginConfig: {},
      processingConfig: {
        fields: [],
        format: [
          'pmtiles'
        ],
        filename: 'capitales',
        label: 'Export PM Tiles',
        dataset: {
          title: 'capitales-du-monde',
          id: 'capitales-du-monde',
          href: 'https://demo.koumoul.com/data-fair/api/v1/datasets/capitales-du-monde'
        },
        filters: []
      },
      tmpDir: 'data/'
    }, config, false, false)
    await exportFile.run(context)
  })

  it.only('should export a pm tiles', async function () {
    // @ts-ignore
    this.timeout(1000000)

    const testsUtils = await import('@data-fair/lib/processings/tests-utils.js')
    const context = testsUtils.context({
      pluginConfig: {},
      processingConfig: {
        fields: [{
          key: 'insee_dep',
          type: 'string'
        }],
        format: [
          'pmtiles'
        ],
        filename: 'departements',
        label: 'Export PM Tiles',
        dataset: {
          title: 'Contours des départements',
          id: 'contours-des-departements',
          href: 'https://koumoul.com/data-fair/api/v1/datasets/contours-des-departements'
        },
        filters: [{
          type: 'in',
          field: {
            key: 'insee_dep',
            type: 'string',
            'x-refersTo': 'http://rdf.insee.fr/def/geo#codeDepartement',
            'x-concept': {
              id: 'codeDepartement',
              title: 'Code département',
              primary: true
            },
            title: 'Code département',
            description: '',
            'x-capabilities': {},
            'x-cardinality': 101
          },
          values: ['35', '56']
        }]
      },
      tmpDir: 'data/'
    }, config, false, false)
    await exportFile.run(context)
  })
})
