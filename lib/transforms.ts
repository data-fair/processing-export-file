import { stringify as wktStringify } from 'wkt'

export interface Field {
  key: string
  type?: string
}

export interface GeomField {
  key: string
}

export function transformLine (line: Record<string, any>, fields: Field[], geomField: GeomField | undefined): void {
  delete line._score

  for (const field of fields) {
    if (line[field.key] === null) line[field.key] = undefined
  }

  if (line._geopoint) {
    const [lat, lon] = String(line._geopoint).split(',')
    line.latitude = lat
    line.longitude = lon
  }

  if (line['_geoshape.coordinates']) {
    line._geoshape = JSON.stringify({
      coordinates: line['_geoshape.coordinates'],
      type: line['_geoshape.type']
    })
  }

  if (geomField && line[geomField.key]) {
    line[geomField.key] = wktStringify(JSON.parse(line[geomField.key]))
  }
}
