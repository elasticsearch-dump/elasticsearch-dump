var _ = require('lodash')

function parseBaseURL (_url, options) {
  var host = _url.replace(/\/+$/, '')
  var hostParts = host.split('/')
  var indexParts = (_.get(options, 'index', '') || '').split('/').filter(Boolean)
  var index
  var type

  if (typeof _.get(options, 'index') === 'string') {
    index = indexParts[0]
    type = indexParts[1]
  } else if (hostParts.length <= 3) {
    //
  } else if (hostParts.length > 4) {
    host = hostParts.slice(0, -2).join('/')
    index = hostParts[hostParts.length - 2]
    type = hostParts[hostParts.length - 1]
  } else {
    host = hostParts.slice(0, -1).join('/')
    index = hostParts[hostParts.length - 1]
  }

  if (_.has(options, 'prefix')) {
    index = (options.prefix || '') + index
  }

  if (_.has(options, 'suffix')) {
    index += (options.suffix || '')
  }

  return {
    url: [host, index, type].filter(Boolean).join('/'),
    host: host,
    index: index,
    type: type
  }
}

module.exports = parseBaseURL
