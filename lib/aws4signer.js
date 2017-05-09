var aws4 = require('aws4')
var AWS = require('aws-sdk')
var path = require('path')
var os = require('os')

var credentials // lazily loaded, see below

var aws4signer = function (esRequest, parent) {
  // Consider deprecating - insecure to use on command line and credentials can be found by default at ~/.aws/credentials or as environment variables
  var useAwsCredentials = ((typeof parent.options.awsAccessKeyId === 'string') && (typeof parent.options.awsSecretAccessKey === 'string'))
  // Consider deprecating - can be achieved with awsChain and setting AWS_PROFILE and AWS_CONFIG_FILE environment variables as needed
  var useAwsProfile = (typeof parent.options.awsIniFileProfile === 'string')
  var useAwsChain = (parent.options.awsChain === true)

  if (useAwsCredentials || useAwsProfile || useAwsChain) {
    // Lazy load credentials object depending on our flavor of credential loading
    // Assumption is that loading only needs to happen once per execution and if refreshing is
    // needed, credentials object should implement credentials.refresh() callback
    if (!credentials) {
      if (useAwsChain) {
        new AWS.CredentialProviderChain().resolve(function (err, resolved) {
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
    var esURL = ''
    if ((esRequest.uri !== undefined) && (esRequest.uri !== null)) {
      esURL = esRequest.uri
    } else if ((esRequest.url !== undefined) && (esRequest.url !== null)) {
      esURL = esRequest.url
    }

    const url = require('url')
    var urlObj = url.parse(esURL)

    esRequest.headers = { 'host': urlObj.hostname }
    esRequest.path = urlObj.path

    aws4.sign(esRequest, credentials)
  }
}

module.exports = aws4signer
