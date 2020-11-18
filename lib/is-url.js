const _ = require('lodash')
const csvUrlRegex = /^(csv:\/\/)(.+)$/

// Naive attempt at detecting a url.
// Should work for elasticdump use case, as file paths
// should not start with http:// or https:// and urls should.
const isUrl = (url) => {
  if (!url) return false
  url = url.toString()
  return url.indexOf('http://') === 0 || url.indexOf('https://') === 0
}

const fromCsvUrl = (filePath) => {
  csvUrlRegex.lastIndex = 0
  const matches = csvUrlRegex.exec(filePath)

  if (!matches) {
    throw new Error(`Invalid CSV Path: ${filePath}`)
  }

  const [protocol, path] = _.tail(matches)

  return {
    protocol,
    path
  }
}

const isCsvUrl = (filePath) => {
  csvUrlRegex.lastIndex = 0
  const matches = csvUrlRegex.exec(filePath)
  return !!matches
}

module.exports = {
  isUrl,
  isCsvUrl,
  fromCsvUrl
}
