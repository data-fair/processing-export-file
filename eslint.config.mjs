import neostandard from 'neostandard'
import dfLibRecommended from '@data-fair/lib-utils/eslint/recommended.js'

export default [
  { ignores: ['config/*', '**/.type/', 'data/', 'node_modules/', 'test/', 'test-it/resources/'] },
  ...dfLibRecommended,
  ...neostandard({ ts: true })
]
