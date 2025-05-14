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

  /**
   * @returns {{version: string, distribution: string, distributionVersion: string}}
   */
  _parseVersion (response) {
    // Skip `this.handleError` if server is AOSS
    // They didn't implement `GET /` and will return 404 here, but we can hard-code
    // the version info by `server` in response header
    if (this.parent.options.openSearchServerless || response.headers.server === 'aoss-amazon') {
      return {
        version: this.parent.options['force-os-version'],
        distribution: 'opensearch-serverless',
        // See https://docs.aws.amazon.com/opensearch-service/latest/developerguide/serverless-overview.html
        // "Currently, serverless collections run OpenSearch version 2.0.x", as of May 13, 2025
        distributionVersion: '2.0.0'
      }
    }

    const err = this.handleError(null, response)
    if (err) {
      throw err
    }

    const parsed = jsonParser.parse(response.body)
    if (!parsed.version) {
      // Fallback version
      this.parent.emit('debug', 'unable to detect version, using default: 5.0.0')
      return {
        version: '5.0.0',
        distribution: 'elasticsearch',
        distributionVersion: '5.0.0'
      }
    }

    if (parsed.version.distribution === 'opensearch') {
      return {
        version: this.parent.options['force-os-version'],
        distribution: 'opensearch',
        distributionVersion: parsed.version.number
      }
    }

    return {
      version: parsed.version.number,
      distribution: parsed.version.build_flavor === 'serverless' ? 'elasticsearch-serverless' : 'elasticserach',
      distributionVersion: parsed.version.number
    }
  }

  version (prefix, callback) {
    if (this.ESversion) {
      return callback()
    }

    const esRequest = {
      url: `${this.base.host}/`,
      method: 'GET'
    }

    aws4signer(esRequest, this.parent).then(() => {
      this.baseRequest(esRequest, (err, response) => {
        if (err) {
          return callback(err, [])
        }

        const parsed = this._parseVersion(response)

        this.parent.emit('debug', `detected ${prefix} distribution = ${parsed.distribution}, ESversion = ${parsed.version}, distribution version = ${parsed.distributionVersion}`)

        // No need to check elasticsearch-serverless. Those are at least >= v8
        if (this.parent.options.searchAfter && parsed.distribution === 'elasticsearch') {
          if (!semver.gte(this.ESFullversion, '7.17.0')) {
            return callback(new Error('searchAfter requires Elasticsearch 7.17.0 or higher'))
          }
        }

        this.ESFullversion = parsed.version
        this.ESversion = this.ESFullversion.split('.')[0]
        this.ESDistribution = parsed.distribution
        this.DistributionVersion = parsed.distributionVersion

        this.setSearchBody()
        callback()
      })
    }).catch(callback)
  }
}

module.exports = Base
