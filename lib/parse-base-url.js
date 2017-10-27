function parseBaseURL (_url, _index) {
  var host = _url.replace(/\/+$/, '')
  var hostParts = host.split('/')
  var indexParts = (_index || '').split('/').filter(Boolean)
  var index
  var type

  if (typeof _index === 'string') {
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

  return {
    url: [host, index, type].filter(Boolean).join('/'),
    host: host,
    index: index,
    type: type
  }
}

module.exports = parseBaseURL
