var aws4 = require('aws4');

aws4signer = function(es_request, parent) {
    var useAwsCredentials = ((typeof parent.options.awsAccessKeyId == 'string') && (typeof parent.options.awsSecretAccessKey == 'string'));

    if (useAwsCredentials) {
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

      aws4.sign(es_request, {
        accessKeyId: parent.options.awsAccessKeyId,
        secretAccessKey: parent.options.awsSecretAccessKey,
        sessionToken: parent.options.sessionToken
      });
    }
};

module.exports = aws4signer;
