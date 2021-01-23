const jsonParser = require('../../jsonparser.js')
const aws4signer = require('../../aws4signer')
const status = require('http-status')
const fs = require('fs')
const agent = require('socks5-http-client/lib/Agent')
const sslAgent = require('socks5-https-client/lib/Agent')

class Base {
  applyProxy (url, proxyHost, proxyPort) {
    let reqAgent = agent

    if (url.indexOf('https://') === 0) {
      reqAgent = sslAgent
    }

    return {
      agentClass: reqAgent,
      agentOptions: {
        socksHost: proxyHost,
        socksPort: proxyPort
      }
    }
  }

  applySSL (props) {
    const options = {}
    const parent = this.parent
    props.forEach(prop => {
      const val = parent.options[prop]
      if (val) {
        const newProp = prop.replace(/^input-/, '').replace(/^output-/, '')
        if (newProp === 'pass') {
          options.passphrase = val
        } else {
          options[newProp] = fs.readFileSync(val)
        }
      }
    })
    return options
  }

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
          this.ESFullversion = response.version.number
          this.ESversion = response.version.number.split('.')[0]
          this.parent.emit('debug', `discovered elasticsearch ${prefix} major version: ${this.ESversion}`)
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
