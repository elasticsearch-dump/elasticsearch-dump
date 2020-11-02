const crypto = require('crypto')

function anonymize (thing, options = {}) {
  if (
    thing === null ||
    thing instanceof Date
  ) {
    return thing
  }

  options = Object.assign({
    domain: 'example.com',
    mobile: '+1-555-123-4567',
    blacklist: []
  }, options)

  if (typeof options.blacklist === 'string') {
    options.blacklist = options.blacklist.split(',')
  }

  switch (typeof thing) {
    case 'object':
      Object
        .keys(thing)
        .reduce(function (object, key) {
          if (options.blacklist.includes(key)) {
            return object
          }

          switch (typeof object[key]) {
            case 'object':
              anonymize(object[key], options)
              break
            case 'string':
              object[key] = anonymize(object[key], options)
              break
            default:
          }

          return object
        }, thing)
      break
    case 'string':
      // If it looks like a date or datetime, leave it alone
      if (/^\d{4}-[01]\d-[0-3]\d(?:[T ][0-2]\d:[0-5]\d:[0-5]\d)?$/.test(thing)) {
        return thing
      }

      return [
        [
          // If it looks like an email, replace it with a hashed variant with the configured domain
          /(([^<>()[\]\\.,;:\s@"]+(\.[^<>()[\]\\.,;:\s@"]+)*)|(".+"))@((\[[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}])|(([a-zA-Z\-0-9]+\.)+[a-zA-Z]{2,}))/ig,
          function (found) {
            return crypto
              .createHash('md5')
              .update(found)
              .digest('hex')
              .slice(0, 11) +
              '@' + options.domain
          }
        ],
        [
          // If it looks like a mobile number, replace it with the configured one
          /[+0][-0-9\s]{6,}[0-9]/g,
          options.mobile
        ]
      ].reduce(function (string, replacement) {
        return string.replace(replacement[0], replacement[1])
      }, thing)
    default:
      return thing
  }
}

module.exports = function (doc, options = {}) {
  anonymize(doc._source, options)
}
