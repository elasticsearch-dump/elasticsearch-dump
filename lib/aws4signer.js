var aws4 = require('aws4')
var awscred = require('awscred')

var aws4signer = function (esRequest, parent) {
  var useAwsCredentials = ((typeof parent.options.awsAccessKeyId === 'string') && (typeof parent.options.awsSecretAccessKey === 'string'))
  var useAwsProfile = (typeof parent.options.awsIniFileProfile === 'string')

  if (useAwsCredentials || useAwsProfile) {
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
  }

  if (useAwsCredentials) {
    aws4.sign(esRequest, {
      accessKeyId: parent.options.awsAccessKeyId,
      secretAccessKey: parent.options.awsSecretAccessKey,
      sessionToken: parent.options.sessionToken
    })
  } else if (useAwsProfile) {
    var awsIniFileName = parent.options.awsIniFileName ? parent.options.awsIniFileName : 'config'
    var creds = awscred.loadProfileFromIniFileSync({profile: parent.options.awsIniFileProfile}, awsIniFileName)
    aws4.sign(esRequest, {
      accessKeyId: creds.aws_access_key_id,
      secretAccessKey: creds.aws_secret_access_key,
      sessionToken: creds.aws_session_token
    })
  }
}

module.exports = aws4signer
