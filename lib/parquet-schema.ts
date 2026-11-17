import type { Field } from './transforms.ts'

const typeConversion: Record<string, string> = {
  integer: 'INT64',
  number: 'FLOAT',
  string: 'UTF8',
  boolean: 'BOOLEAN'
}

export interface ParquetFieldDef {
  type: string
  optional: true
  compression: 'ZSTD'
}

export function buildParquetSchemaDefinition (fields: Field[]): Record<string, ParquetFieldDef> {
  const def: Record<string, ParquetFieldDef> = {}
  for (const field of fields) {
    if (!field.type) continue
    // parquetjs writes no dictionary, uncompressed columns are ~5 times bigger. ZSTD is the native
    // zlib codec of Node (outside the JS heap), read by pyarrow, DuckDB, Polars and Spark.
    def[field.key] = { type: typeConversion[field.type], optional: true, compression: 'ZSTD' }
  }
  return def
}
