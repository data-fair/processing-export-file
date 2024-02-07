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
          title: 'prenoms-test',
          id: 'hl4q7qtv2ahuqtbobh65mlzx',
          href: 'https://staging-koumoul.com/data-fair/api/v1/datasets/hl4q7qtv2ahuqtbobh65mlzx'
        },
        fields: [
          'sexe',
          'preusuel',
          'nombre'
        ],
        label: 'Mon export',
        filename: 'my-export'
      },
      tmpDir: 'data/'
    }, config, false)
    await exportFile.run(context)
  })
})
