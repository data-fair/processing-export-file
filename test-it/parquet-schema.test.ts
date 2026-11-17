import { strict as assert } from 'node:assert'
import { describe, it } from 'node:test'
import { buildParquetSchemaDefinition } from '../lib/parquet-schema.ts'

describe('buildParquetSchemaDefinition', () => {
  it('maps DF types to parquet primitives', () => {
    const schema = buildParquetSchemaDefinition([
      { key: 'a', type: 'integer' },
      { key: 'b', type: 'number' },
      { key: 'c', type: 'string' },
      { key: 'd', type: 'boolean' }
    ])
    assert.deepEqual(schema, {
      a: { type: 'INT64', optional: true, compression: 'ZSTD' },
      b: { type: 'FLOAT', optional: true, compression: 'ZSTD' },
      c: { type: 'UTF8', optional: true, compression: 'ZSTD' },
      d: { type: 'BOOLEAN', optional: true, compression: 'ZSTD' }
    })
  })
})
