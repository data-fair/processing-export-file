// Override these values locally by creating a config/local-test.mjs file (gitignored)
export default {
  /** Base URL of the data-fair instance used for tests. Leave as http://test.data-fair.local for nock-based local tests. */
  dataFairUrl: 'http://test.data-fair.local',
  /** API key for data-fair, irrelevant when using nock. */
  dataFairAPIKey: 'test-api-key'
}
