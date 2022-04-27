function parseBaseURL (_url, options = {}) {
  let host = _url.replace(/\/+$/, '')
  const hostParts = host.split('/')
  let index
  const indexParts = (options.index || '').split('/').filter(Boolean)
  let type

  if (typeof options.index === 'string') {
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

  if (index) {
    index = `${options.prefix || ''}${index}${options.suffix || ''}`
  }

  return {
    url: [host, index, type].filter(Boolean).join('/'),
    host,
    index,
    type
  }
}

module.exports = parseBaseURL
