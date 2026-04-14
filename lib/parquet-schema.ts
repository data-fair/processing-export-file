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
}

export function buildParquetSchemaDefinition (fields: Field[]): Record<string, ParquetFieldDef> {
  const def: Record<string, ParquetFieldDef> = {}
  for (const field of fields) {
    if (!field.type) continue
    def[field.key] = { type: typeConversion[field.type], optional: true }
  }
  return def
}
