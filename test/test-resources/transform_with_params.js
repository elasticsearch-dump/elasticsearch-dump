const crypto = require('crypto')

module.exports = function (doc, params) {
  doc._source[params.targetField] = crypto
    .createHash('md5')
    .update(String(doc._source[params.sourceField]))
    .digest('hex')
}
