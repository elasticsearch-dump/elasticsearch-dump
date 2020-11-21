const aws4 = require('aws4')
const AWS = require('aws-sdk')
const path = require('path')
const os = require('os')

let credentials // lazily loaded, see below

const aws4signer = (esRequest, parent) => {
  // Consider deprecating - insecure to use on command line and credentials can be found by default at ~/.aws/credentials or as environment variables
  const useAwsCredentials = ((typeof parent.options.awsAccessKeyId === 'string') && (typeof parent.options.awsSecretAccessKey === 'string'))
  // Consider deprecating - can be achieved with awsChain and setting AWS_PROFILE and AWS_CONFIG_FILE environment variables as needed
  const useAwsProfile = (typeof parent.options.awsIniFileProfile === 'string')
  const useAwsChain = (parent.options.awsChain === true)
  const awsUrlRegex = new RegExp(parent.options.awsUrlRegex || /^https?:\/\/.*\.amazonaws\.com.*$/)

  if (!awsUrlRegex.test(esRequest.url) && !awsUrlRegex.test(esRequest.uri)) {
    return
  }

  if (useAwsCredentials || useAwsProfile || useAwsChain) {
    // Lazy load credentials object depending on our flavor of credential loading
    // Assumption is that loading only needs to happen once per execution and if refreshing is
    // needed, credentials object should implement credentials.refresh() callback
    if (!credentials) {
      if (useAwsChain) {
        new AWS.CredentialProviderChain().resolve((err, resolved) => {
          if (err) { throw err } else {
            credentials = resolved
          }
        })
      } else if (useAwsCredentials) {
        credentials = {
          accessKeyId: parent.options.awsAccessKeyId,
          secretAccessKey: parent.options.awsSecretAccessKey,
          sessionToken: parent.options.sessionToken
        }
      } else if (useAwsProfile) {
        credentials = new AWS.SharedIniFileCredentials({
          profile: parent.options.awsIniFileProfile,
          filename: path.join(os.homedir(), '.aws', parent.options.awsIniFileName ? parent.options.awsIniFileName : 'config')
        })
      }
    }

    // get aws required stuff from uri or url
    let esURL = ''
    if ((esRequest.uri !== undefined) && (esRequest.uri !== null)) {
      esURL = esRequest.uri
    } else if ((esRequest.url !== undefined) && (esRequest.url !== null)) {
      esURL = esRequest.url
    }

    const urlObj = new URL(esURL)

    if (parent.options.awsService) {
      esRequest.service = parent.options.awsService
    }

    if (parent.options.awsRegion) {
      esRequest.region = parent.options.awsRegion
    }

    esRequest.headers = Object.assign({ host: urlObj.hostname, 'Content-Type': 'application/json' }, esRequest.headers)
    esRequest.path = `${urlObj.pathname}?${urlObj.searchParams.toString()}`
    aws4.sign(esRequest, credentials)
  }
}

module.exports = aws4signer
