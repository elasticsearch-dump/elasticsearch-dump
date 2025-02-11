const jsonParser = require('../../jsonparser.js')
const aws4signer = require('../../aws4signer')
const req = require('../../request')
const status = require('http-status')
const semver = require('semver')

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

  setSearchBody () {
    if (!this.searchBody) {
      if (this.ESversion >= 5) {
        this.searchBody = { query: { match_all: {} }, stored_fields: ['*'], _source: true }
      } else {
        this.searchBody = { query: { match_all: {} }, fields: ['*'], _source: true }
      }
    }
  }

  version (prefix, callback) {
    if (this.ESversion) {
      return callback()
    }

    if (this.parent.options.openSearchServerless) {
      this.ESversion = 2 // OpenSearch serverless default version
      this.parent.emit('debug', `opensearch serverless, using version: ${this.ESversion}`)
      this.setSearchBody()
      return callback()
    }

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

        const parsed = jsonParser.parse(response.body)

        // Set version based on response
        if (parsed.version) {
          const isOpenSearch = parsed.version.distribution === 'opensearch'
          this.ESFullversion = isOpenSearch
            ? this.parent.options['force-os-version']
            : parsed.version.number
          this.ESversion = this.ESFullversion.split('.')[0]

          // Version check for searchAfter
          if (this.parent.options.searchAfter && !isOpenSearch) {
            if (!semver.gte(this.ESFullversion, '7.17.0')) {
              return callback(new Error('searchAfter requires Elasticsearch 7.17.0 or higher'))
            }
          }

          this.parent.emit('debug', `detected ${prefix} ${isOpenSearch ? 'opensearch' : 'elasticsearch'} version: ${this.ESversion}`)
        } else {
          this.ESversion = 5 // Fallback version
          this.parent.emit('debug', `unable to detect version, using default: ${this.ESversion} for ${prefix}`)
        }

        this.setSearchBody()
        callback()
      })
    }).catch(callback)
  }
}

module.exports = Base
