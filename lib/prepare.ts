import type { PrepareFunction } from '@data-fair/lib-common-types/processings.js'
import type { ProcessingConfig } from '#types/processingConfig/index.ts'

// this processing has no secret to move out of its config
const prepare: PrepareFunction<ProcessingConfig> = async ({ processingConfig, secrets }) => {
  return { processingConfig, secrets }
}

export default prepare
