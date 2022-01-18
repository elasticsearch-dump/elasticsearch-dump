const { isUrl, isCsvUrl, isS3Prefix } = require('./is-url')
const addAuth = require('./add-auth')
const path = require('path')
const s3urls = require('s3urls')

const getIo = (elasticdump, type) => {
  let EntryProto
  const transportPath = elasticdump.options[type]
  const transport = elasticdump.options[`${type}Transport`]
  if (transportPath && !transport) {
    if (isUrl(transportPath)) {
      elasticdump[`${type}Type`] = 'elasticsearch'
      if (elasticdump.options.httpAuthFile) {
        elasticdump.options[type] = addAuth(transportPath, elasticdump.options.httpAuthFile)
      }
    } else if (isS3Prefix && s3urls.valid(transportPath)) {
      elasticdump[`${type}Type`] = 's3'
    } else if (isCsvUrl(transportPath)) {
      elasticdump[`${type}Type`] = 'csv'
    } else {
      elasticdump[`${type}Type`] = 'file'
    }

    const inputOpts = {
      index: elasticdump.options[`${type}-index`],
      headers: elasticdump.options.headers,
      type
    }

    if (type === 'output') {
      Object.assign(inputOpts, {
        prefix: elasticdump.options.prefix,
        suffix: elasticdump.options.suffix
      })
    }

    EntryProto = require(path.join(__dirname, 'transports', elasticdump[`${type}Type`]))[elasticdump[`${type}Type`]]
    elasticdump[type] = (new EntryProto(elasticdump, transportPath, inputOpts))
  } else if (transport) {
    elasticdump[`${type}Type`] = String(transport)
    EntryProto = require(transport) 
    const EntryProtoKeys = Object.keys(EntryProto)
    elasticdump[type] = (new EntryProto[EntryProtoKeys[0]](elasticdump, transportPath, elasticdump.options[`${type}-index`]))
  }
}

module.exports = getIo
