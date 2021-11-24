const AWS = require('aws-sdk')
const initAws = (options) => {
  AWS.config.update({
    accessKeyId: options.s3AccessKeyId,
    secretAccessKey: options.s3SecretAccessKey,
    sslEnabled: options.s3SSLEnabled,
    s3ForcePathStyle: options.s3ForcePathStyle
  })
  if (options.s3Endpoint != null) {
    AWS.config.update({
      endpoint: options.s3Endpoint
    })
  }
  if (options.s3Region != null) {
    AWS.config.update({
      region: options.s3Region
    })
  }
  if (options.debug) {
    AWS.config.update({
      logger: 'process.stdout'
    })
  }
  if (options.retryAttempts > 0) {
    AWS.config.update({
      maxRetries: options.retryAttempts
    })
  }
  if (options.retryDelayBase > 0) {
    AWS.config.update({
      retryDelayOptions: { base: options.retryDelayBase }
    })
  }
  if (options.customBackoff) {
    AWS.config.update({
      retryDelayOptions: {
        customBackoff: retryCount => Math.max(retryCount * 100, 3000)
      }
    })
  }
  if (options.s3Configs) {
    AWS.config.update(options.s3Configs)
  }
}

module.exports = initAws
