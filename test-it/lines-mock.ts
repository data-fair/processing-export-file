/** Matches the page requests of /lines, not the count request (size=0). */
export const isPage = (q: any) => q.size !== '0'

const cell = (v: unknown): string => {
  if (v === null || v === undefined) return ''
  if (typeof v === 'string') return '"' + v.replace(/"/g, '""') + '"'
  if (typeof v === 'boolean') return v ? '1' : '0'
  return String(v)
}

/**
 * nock reply serving a /lines page in the format asked by the request: the JSON envelope,
 * or a CSV page written like data-fair does (BOM, quoted strings, 1/0 booleans, Link header).
 */
export const linesPage = (page: { total: number, next?: string, results: Record<string, any>[] }) =>
  (uri: string | { url: string }): [number, any, Record<string, string>?] => {
    const q = new URL(typeof uri === 'string' ? uri : uri.url, 'http://localhost').searchParams
    if (q.get('format') !== 'csv') return [200, page]
    const keys = q.get('select')!.split(',')
    const body = '﻿' + page.results.map(r => keys.map(k => cell(r[k])).join(',') + '\n').join('')
    if (!page.next) return [200, body, {}]
    // like data-fair, the next link keeps the query of the request
    const next = new URL(page.next)
    for (const key of ['format', 'header', 'select']) next.searchParams.set(key, q.get(key)!)
    return [200, body, { link: `<${next.href}>; rel="next"` }]
  }
