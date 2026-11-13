declare module 'wkt' {
  export function stringify (geometry: unknown): string
  export function parse (wkt: string): unknown
}

declare module 'archiver' {
  import type { Transform } from 'node:stream'
  export class ZipArchive extends Transform {
    constructor (options?: Record<string, unknown>)
    directory (dirpath: string, destpath: string | false): this
    finalize (): Promise<void>
  }
}
