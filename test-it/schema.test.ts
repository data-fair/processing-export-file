import { strict as assert } from 'node:assert'
import { describe, it } from 'node:test'
import pluginConfigSchema from '../plugin-config-schema.json' with { type: 'json' }
import processingConfigSchema from '../processing-config-schema.json' with { type: 'json' }

describe('Export file — schemas', () => {
  it('exposes a plugin config schema', () => {
    assert.equal(pluginConfigSchema.type, 'object')
  })

  it('exposes a processing config schema with tabs display', () => {
    assert.equal(processingConfigSchema.type, 'object')
    assert.equal(processingConfigSchema['x-display'], 'tabs')
    assert.ok(Array.isArray(processingConfigSchema.allOf))
  })

  it('allows the 7 export formats', () => {
    const formatNode = processingConfigSchema.allOf
      .flatMap((section: any) => Object.entries(section.properties ?? {}))
      .find(([key]) => key === 'format')?.[1] as any
    assert.ok(formatNode, 'format property not found')
    assert.deepEqual(
      formatNode.items.enum.sort(),
      ['csv', 'geojson', 'gpkg', 'parquet', 'pmtiles', 'shp', 'xlsx']
    )
  })
})
