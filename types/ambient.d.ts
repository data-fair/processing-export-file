declare module 'wkt' {
  export function stringify (geometry: unknown): string
  export function parse (wkt: string): unknown
}
