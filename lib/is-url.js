const _ = require('lodash')
const csvUrlRegex = /^(csv:\/\/)(.+)$/
const s3Regex = /^(s3:\/\/)(.+)$/

// Naive attempt at detecting a url.
// Should work for elasticdump use case, as file paths
// should not start with http:// or https:// and urls should.
const isUrl = (url) => {
  if (!url) return false
  url = decodeURIComponent(encodeURIOnce(url.toString()))
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

const isS3Prefix = (filePath) => {
  s3Regex.lastIndex = 0
  const matches = s3Regex.exec(filePath)
  return !!matches
}

const encodeURIOnce = (uri) => {
  let isEncoded

  try {
    isEncoded = uri !== decodeURIComponent(uri)
  } catch (error) {
    if (error instanceof URIError) {
      isEncoded = false
    } else {
      throw error
    }
  }

  return isEncoded ? uri : encodeURIComponent(uri)
}

module.exports = {
  isUrl,
  isCsvUrl,
  isS3Prefix,
  fromCsvUrl,
  encodeURIOnce
}
