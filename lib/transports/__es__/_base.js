const jsonParser = require('../../jsonparser.js')
const aws4signer = require('../../aws4signer')
const req = require('../../request')
const status = require('http-status')

class Base {
  applyProxy (url, proxyHost, proxyPort) { return req.applyProxy(url, proxyHost, proxyPort) }

  applySSL (props) { return req.applySSL(props, this) }

  handleError (err, response) {
    if (err) return err
    if (response.statusCode !== 200) {
      err = new Error(response.body)
      err.statusCode = response.statusCode
      err.name = status[`${response.statusCode}_NAME`]
      if (!err.message) {
        err.message = status[`${response.statusCode}_MESSAGE`]
      }
      return err
    }
  }

  paramsToString (paramsObj, prefix = '?') {
    return paramsObj ? `${prefix}${Object.keys(paramsObj).map(key => `${key}=${paramsObj[key]}`).join('&')}` : ''
  }

  version (prefix, callback) {
    if (this.ESversion) { return callback() }
    const esRequest = {
      url: `${this.base.host}/`,
      method: 'GET'
    }
    aws4signer(esRequest, this.parent).then(() => {
      this.baseRequest(esRequest, (err, response) => {
        err = this.handleError(err, response)
        if (err) {
          return callback(err, [])
        }

        response = jsonParser.parse(response.body)

        if (response.version) {
          if (response.version.distribution && response.version.distribution === 'opensearch') {
            this.ESFullversion = this.parent.options['force-os-version']
            this.ESversion = this.ESFullversion.split('.')[0]
            this.parent.emit('debug', `distribution is opensearch, assuming major version: ${this.ESversion}`)
          } else {
            this.ESFullversion = response.version.number
            this.ESversion = response.version.number.split('.')[0]
            this.parent.emit('debug', `discovered elasticsearch ${prefix} major version: ${this.ESversion}`)
          }
        } else {
          this.ESversion = 5
          this.parent.emit('debug', `cannot discover elasticsearch ${prefix} major version, assuming: ${this.ESversion}`)
        }

        if (!this.searchBody) {
          if (this.ESversion >= 5) {
            this.searchBody = { query: { match_all: {} }, stored_fields: ['*'], _source: true }
          } else {
            this.searchBody = { query: { match_all: {} }, fields: ['*'], _source: true }
          }
        }

        callback()
      })
    }).catch(callback)
  }
}

module.exports = Base
