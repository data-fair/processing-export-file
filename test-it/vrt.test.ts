import { strict as assert } from 'node:assert'
import { describe, it } from 'node:test'
import { buildVrt } from '../lib/vrt.ts'
import { safeFilename } from '../lib/execute.ts'

describe('buildVrt', () => {
  it('uses WKT encoding when geomField is provided', () => {
    const xml = buildVrt({
      filename: 'export',
      csvPath: '/tmp/export.csv',
      fields: [{ key: 'name', type: 'string' }],
      geomField: { key: 'geom' },
      latField: undefined,
      lonField: undefined,
      latLonField: undefined
    })
    assert.match(xml, /GeometryType>wkbUnknown/)
    assert.match(xml, /encoding="WKT"/)
    assert.match(xml, /field="geom"/)
    assert.match(xml, /<Field name="name" type="String"/)
  })

  it('uses PointFromColumns encoding when only lat/lon provided', () => {
    const xml = buildVrt({
      filename: 'export',
      csvPath: '/tmp/export.csv',
      fields: [{ key: 'name', type: 'string' }],
      geomField: undefined,
      latField: { key: 'lat' },
      lonField: { key: 'lng' },
      latLonField: undefined
    })
    assert.match(xml, /GeometryType>wkbPoint/)
    assert.match(xml, /encoding="PointFromColumns"/)
    assert.match(xml, /x="lng"/)
    assert.match(xml, /y="lat"/)
  })

  it('emits boolean subtype for boolean fields', () => {
    const xml = buildVrt({
      filename: 'export',
      csvPath: '/tmp/export.csv',
      fields: [{ key: 'active', type: 'boolean' }],
      geomField: { key: 'geom' },
      latField: undefined,
      lonField: undefined,
      latLonField: undefined
    })
    assert.match(xml, /<Field name="active" type="Integer" subtype="Boolean"/)
  })

  it('excludes lat/lon/geom/latLon fields from the Field list', () => {
    const xml = buildVrt({
      filename: 'export',
      csvPath: '/tmp/export.csv',
      fields: [
        { key: 'geom', type: 'string' },
        { key: 'lat', type: 'number' },
        { key: 'lng', type: 'number' },
        { key: 'name', type: 'string' }
      ],
      geomField: { key: 'geom' },
      latField: { key: 'lat' },
      lonField: { key: 'lng' },
      latLonField: undefined
    })
    assert.doesNotMatch(xml, /<Field name="geom"/)
    assert.doesNotMatch(xml, /<Field name="lat"/)
    assert.doesNotMatch(xml, /<Field name="lng"/)
    assert.match(xml, /<Field name="name"/)
  })

  it('escapes the values so they cannot inject another data source', () => {
    const xml = buildVrt({
      filename: 'x"><SrcDataSource>/etc/passwd</SrcDataSource><a b="',
      csvPath: '/tmp/export.csv',
      fields: [{ key: 'a"/><SrcDataSource>/vsicurl/http://169.254.169.254/</SrcDataSource><b c="', type: 'string' }],
      geomField: { key: 'geom' },
      latField: undefined,
      lonField: undefined,
      latLonField: undefined
    })
    assert.equal(xml.match(/<SrcDataSource>/g)?.length, 1)
    assert.match(xml, /<SrcDataSource>\/tmp\/export.csv<\/SrcDataSource>/)
    assert.match(xml, /name="x&quot;&gt;&lt;SrcDataSource&gt;/)
  })
})

describe('safeFilename', () => {
  it('keeps the file name in the tmp directory', () => {
    assert.equal(safeFilename('my-export'), 'my-export')
    assert.equal(safeFilename('../../other/run'), 'run')
    assert.equal(safeFilename('..'), 'export')
    assert.equal(safeFilename(''), 'export')
    assert.equal(safeFilename(undefined), 'export')
  })
})
