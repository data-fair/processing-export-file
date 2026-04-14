import type { Field, GeomField } from './transforms.ts'

const typeToVrtType: Record<string, string> = {
  integer: 'Integer',
  number: 'Real',
  string: 'String',
  boolean: 'Integer'
}

export interface BuildVrtOpts {
  filename: string
  csvPath: string
  fields: Field[]
  geomField: GeomField | undefined
  latField: Field | undefined
  lonField: Field | undefined
  latLonField: Field | undefined
}

export function buildVrt (opts: BuildVrtOpts): string {
  const { filename, csvPath, fields, geomField, latField, lonField, latLonField } = opts

  const geometryType = geomField ? 'wkbUnknown' : 'wkbPoint'
  const encoding = geomField ? 'WKT' : 'PointFromColumns'
  const geometryField = geomField
    ? `field="${geomField.key}"`
    : `x="${lonField?.key ?? 'longitude'}" y="${latField?.key ?? 'latitude'}"`

  const excludedKeys = new Set([latField?.key, lonField?.key, geomField?.key, latLonField?.key].filter(Boolean))
  const fieldLines = fields
    .filter(f => !excludedKeys.has(f.key))
    .map(f => {
      const vrtType = typeToVrtType[f.type ?? 'string'] ?? 'String'
      const subtype = f.type === 'boolean' ? 'Boolean' : 'None'
      return `<Field name="${f.key}" type="${vrtType}" subtype="${subtype}"/>`
    })
    .join('\n        ')

  return `<OGRVRTDataSource>
    <OGRVRTLayer name="${filename}">
        <SrcDataSource>${csvPath}</SrcDataSource>
        <GeometryType>${geometryType}</GeometryType>
        <LayerSRS>WGS84</LayerSRS>
        <GeometryField encoding="${encoding}" ${geometryField} />
        ${fieldLines}
    </OGRVRTLayer>
</OGRVRTDataSource>`
}
