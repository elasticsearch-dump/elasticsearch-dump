const isUrl = require('./is-url')
const addAuth = require('./add-auth')
const path = require('path')

const getIo = (elasticdump, type) => {
  let EntryProto
  if (elasticdump.options[type] && !elasticdump[`${type}Transport`]) {
    if (type === 'output' && elasticdump.options.s3Bucket) {
      elasticdump[`${type}Type`] = 's3'
    } else if (isUrl(elasticdump.options[type])) {
      elasticdump[`${type}Type`] = 'elasticsearch'
      if (elasticdump.options.httpAuthFile) {
        elasticdump.options[type] = addAuth(elasticdump.options[type], elasticdump.options.httpAuthFile)
      }
    } else {
      elasticdump[`${type}Type`] = 'file'
    }

    const inputOpts = {
      index: elasticdump.options[`${type}-index`],
      headers: elasticdump.options['headers']
    }

    if (type === 'output') {
      Object.assign(inputOpts, {
        prefix: elasticdump.options['prefix'],
        suffix: elasticdump.options['suffix']
      })
    }

    EntryProto = require(path.join(__dirname, 'transports', elasticdump[`${type}Type`]))[elasticdump[`${type}Type`]]
    elasticdump[type] = (new EntryProto(elasticdump, elasticdump.options[type], inputOpts))
  } else if (elasticdump.options[`${type}Transport`]) {
    elasticdump[`${type}Type`] = String(elasticdump.options[`${type}Transport`])
    EntryProto = require(elasticdump.options[`${type}Transport`])
    const EntryProtoKeys = Object.keys(EntryProto)
    elasticdump[type] = (new EntryProto[EntryProtoKeys[0]](elasticdump, elasticdump.options[type], elasticdump.options[`${type}-index`]))
  }
}

module.exports = getIo
