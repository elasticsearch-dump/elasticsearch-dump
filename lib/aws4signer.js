var aws4 = require('aws4');
var awscred = require('awscred');

aws4signer = function(es_request, parent) {
    var useAwsCredentials = ((typeof parent.options.awsAccessKeyId == 'string') && (typeof parent.options.awsSecretAccessKey == 'string'));
    var useAwsProfile = (typeof parent.options.awsIniFileProfile == 'string');
    
    if (useAwsCredentials || useAwsProfile) {
      // get aws required stuff from uri or url
      var es_url = '';
      if ((es_request.uri !== undefined) && (es_request.uri !== null)) {
        es_url = es_request.uri;
      } else if((es_request.url !== undefined) && (es_request.url !== null)) {
        es_url = es_request.url;
      }

      const url = require('url');
      var urlObj = url.parse(es_url);
      
      es_request.headers =  { 'host': urlObj.hostname};
      es_request.path = urlObj.path;
    }
  
    if (useAwsCredentials) {
      aws4.sign(es_request, {
        accessKeyId: parent.options.awsAccessKeyId,
        secretAccessKey: parent.options.awsSecretAccessKey,
        sessionToken: parent.options.sessionToken
      });
    } else if (useAwsProfile) {
      var awsIniFileName = parent.options.awsIniFileName ? parent.options.awsIniFileName : 'config';
      var creds = awscred.loadProfileFromIniFileSync({profile: parent.options.awsIniFileProfile}, awsIniFileName);
      aws4.sign(es_request, {
        accessKeyId: creds.aws_access_key_id,
        secretAccessKey: creds.aws_secret_access_key,
        sessionToken: creds.aws_session_token
      });
    }
};

module.exports = aws4signer;
