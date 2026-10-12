import type { Field, GeomField } from './transforms.ts'

const typeToVrtType: Record<string, string> = {
  integer: 'Integer',
  number: 'Real',
  string: 'String',
  boolean: 'Integer'
}

// the values come from the processing config and from the dataset schema,
// unescaped they could inject other data sources (local files, urls) in the VRT
export const escapeXml = (value: string): string => String(value)
  .replace(/&/g, '&amp;')
  .replace(/</g, '&lt;')
  .replace(/>/g, '&gt;')
  .replace(/"/g, '&quot;')
  .replace(/'/g, '&apos;')

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
    ? `field="${escapeXml(geomField.key)}"`
    : `x="${escapeXml(lonField?.key ?? 'longitude')}" y="${escapeXml(latField?.key ?? 'latitude')}"`

  const excludedKeys = new Set([latField?.key, lonField?.key, geomField?.key, latLonField?.key].filter(Boolean))
  const fieldLines = fields
    .filter(f => !excludedKeys.has(f.key))
    .map(f => {
      const vrtType = typeToVrtType[f.type ?? 'string'] ?? 'String'
      const subtype = f.type === 'boolean' ? 'Boolean' : 'None'
      return `<Field name="${escapeXml(f.key)}" type="${vrtType}" subtype="${subtype}"/>`
    })
    .join('\n        ')

  return `<OGRVRTDataSource>
    <OGRVRTLayer name="${escapeXml(filename)}">
        <SrcDataSource>${escapeXml(csvPath)}</SrcDataSource>
        <GeometryType>${geometryType}</GeometryType>
        <LayerSRS>WGS84</LayerSRS>
        <GeometryField encoding="${encoding}" ${geometryField} />
        ${fieldLines}
    </OGRVRTLayer>
</OGRVRTDataSource>`
}
