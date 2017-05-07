var crypto = require('crypto')

module.exports = function (doc) {
  doc._source.bar = crypto
    .createHash('md5')
    .update(doc._source.foo)
    .digest('hex')
}
