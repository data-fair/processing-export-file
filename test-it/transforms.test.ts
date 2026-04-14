import { strict as assert } from 'node:assert'
import { describe, it } from 'node:test'
import { transformLine } from '../lib/transforms.ts'

describe('transformLine', () => {
  it('deletes _score', () => {
    const line: any = { _score: 0.9, name: 'a' }
    transformLine(line, [{ key: 'name', type: 'string' }], undefined)
    assert.equal(line._score, undefined)
  })

  it('converts null to undefined for declared fields', () => {
    const line: any = { name: null }
    transformLine(line, [{ key: 'name', type: 'string' }], undefined)
    assert.equal(line.name, undefined)
  })

  it('splits _geopoint into latitude + longitude', () => {
    const line: any = { _geopoint: '48.85,2.35' }
    transformLine(line, [], undefined)
    assert.equal(line.latitude, '48.85')
    assert.equal(line.longitude, '2.35')
  })

  it('flattens _geoshape coordinates + type into JSON string', () => {
    const line: any = { '_geoshape.coordinates': [[1, 2], [3, 4]], '_geoshape.type': 'LineString' }
    transformLine(line, [], undefined)
    assert.equal(line._geoshape, JSON.stringify({ coordinates: [[1, 2], [3, 4]], type: 'LineString' }))
  })

  it('converts geometry field to WKT when geomField provided', () => {
    const line: any = { geom: '{"type":"Point","coordinates":[1,2]}' }
    transformLine(line, [{ key: 'geom', type: 'string' }], { key: 'geom' })
    assert.equal(line.geom, 'POINT (1 2)')
  })
})
