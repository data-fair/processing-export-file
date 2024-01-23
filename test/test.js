process.env.NODE_ENV = 'test'
const config = require('config')
const assert = require('assert').strict
const exportFile = require('../')
const testUtils = require('@data-fair/processings-test-utils')

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

    const context = testUtils.context({
      pluginConfig: {},
      processingConfig: {
        dataset: {
          title: 'gares peage 2023 shp',
          id: '3oe9erji6z9r6bwg6tcldyu8',
          href: 'https://staging-koumoul.com/data-fair/api/v1/datasets/3oe9erji6z9r6bwg6tcldyu8'
        },
        fields: [
          'daterefere',
          'route',
          'pr',
          'deppr',
          '_geopoint'
        ],
        label: 'Mon export',
        filename: 'my-export'
      },
      tmpDir: 'data/'
    }, config, false)
    await exportFile.run(context)
  })
})
