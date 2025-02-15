const AWS = require('aws-sdk')

const initAws = (options) => {
  // Base AWS config
  const config = {
    accessKeyId: options.s3AccessKeyId,
    secretAccessKey: options.s3SecretAccessKey,
    sessionToken: options.s3SessionToken,
    sslEnabled: options.s3SSLEnabled,
    s3ForcePathStyle: options.s3ForcePathStyle
  }

  // Optional configurations
  if (options.s3Endpoint) config.endpoint = options.s3Endpoint
  if (options.s3Region) config.region = options.s3Region
  if (options.debug) config.logger = 'process.stdout'
  if (options.retryAttempts > 0) config.maxRetries = options.retryAttempts

  // Retry options
  const retryOptions = {}
  if (options.retryDelayBase > 0) retryOptions.base = options.retryDelayBase
  if (options.customBackoff) {
    retryOptions.customBackoff = retryCount => Math.max(retryCount * 100, 3000)
  }
  if (Object.keys(retryOptions).length > 0) {
    config.retryDelayOptions = retryOptions
  }

  // Apply s3Configs last to allow overrides
  AWS.config.update({
    ...config,
    ...(options.s3Configs || {})
  })
}

module.exports = initAws
