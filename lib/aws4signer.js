var aws4 = require('aws4');

aws4signer = function(es_request, awsAccessKeyId, awsSecretAccessKey) {
    var useAwsCredentials = ((typeof awsAccessKeyId == 'string') && (typeof awsSecretAccessKey == 'string'));

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

      aws4.sign(es_request, {"accessKeyId": awsAccessKeyId, "secretAccessKey": awsSecretAccessKey});
    }
};

module.exports = aws4signer;