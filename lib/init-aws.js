const AWS = require('aws-sdk')
const http = require('http')
const https = require('https')

const initAws = (options) => {
  // Create HTTP agents with connection pooling
  const agentConfig = {
    keepAlive: true,
    keepAliveMsecs: 30000,
    maxSockets: options.maxSockets || 50,
    maxFreeSockets: 10
  }

  const httpAgent = new http.Agent(agentConfig)
  const httpsAgent = new https.Agent(agentConfig)
  const useHttps = !options.s3Endpoint || options.s3Endpoint.startsWith('https')

  // AWS configuration
  const config = {
    accessKeyId: options.s3AccessKeyId,
    secretAccessKey: options.s3SecretAccessKey,
    sessionToken: options.s3SessionToken,
    sslEnabled: options.s3SSLEnabled,
    s3ForcePathStyle: options.s3ForcePathStyle,
    httpOptions: {
      timeout: 300000,
      agent: useHttps ? httpsAgent : httpAgent
    },
    httpAgent,
    httpsAgent,
    maxRetries: options.retryAttempts || 3
  }

  if (options.s3Endpoint) config.endpoint = options.s3Endpoint
  if (options.s3Region) config.region = options.s3Region
  if (options.debug) config.logger = process.stdout

  // Custom retry backoff
  if (options.customBackoff || options.retryDelayBase) {
    config.retryDelayOptions = {
      ...(options.retryDelayBase && { base: options.retryDelayBase }),
      ...(options.customBackoff && {
        customBackoff: retryCount => Math.max(retryCount * 100, 3000)
      })
    }
  }

  AWS.config.update({ ...config, ...(options.s3Configs || {}) })
}

module.exports = initAws
