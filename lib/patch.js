// detect node version
const url = require('url')
const _version = Number(process.version.match(/^v(\d+\.\d+)/)[1])

// this patch will be removed when support for node v8 is dropped
if (_version < 10 && _version > 7) {
  global.URL = url.URL
}
