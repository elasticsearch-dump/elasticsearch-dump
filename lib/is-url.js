module.exports = isUrl

// Naive attempt at detecting a url.
// Should work for elasticdump use case, as file paths
// should not start with http:// or https:// and urls should.
function isUrl (url) {
  if (!url) return false
  url = url.toString()
  return url.indexOf('http://') === 0 || url.indexOf('https://') === 0
}
