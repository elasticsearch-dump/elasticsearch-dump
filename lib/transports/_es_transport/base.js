const request = require('requestretry')
const parseBaseURL = require('../../parse-base-url')
const _ = require('lodash')
const fs = require('fs')
const agent = require('socks5-http-client/lib/Agent')
const sslAgent = require('socks5-https-client/lib/Agent')

class Base {
  constructor (parent, url, options) {
    this.base = parseBaseURL(url, options)
    this.parent = parent
    this.lastScrollId = null
    this.settingsExclude = ['settings.index.version', 'settings.index.creation_date', 'settings.index.uuid', 'settings.index.provided_name']
    /**
     * Note that _parent, has been deprecated since ES 6.0
     * Note that _timestamp & _ttl have been deprecated since ES 5.0
     */
    this.defaultMetaFields = ['routing', 'parent', 'timestamp', 'ttl']
    this.totalSearchResults = 0
    this.elementsToSkip = 0
    this.searchBody = this.parent.options.searchBody
    this.ESversion = null
    this.ESFullversion = null
    this.featureFlag = false

    const defaultOptions = {
      timeout: this.parent.options.timeout,
      headers: Object.assign({
        'User-Agent': 'elasticdump',
        'Content-Type': 'application/json'
      }, options.headers),
      maxAttempts: this.parent.options.retryAttempts || 5, // (default) try 5 times
      retryDelay: this.parent.options.retryDelay || 5000, // (default) wait for 5s before trying again
      retryStrategy: request.RetryStrategies.HTTPOrNetworkError // (default) retry on 5xx or network errors
    }

    if (parent.options[`${options.type}SocksProxy`]) {
      Object.assign(defaultOptions,
        this.applyProxy(url, parent.options[`${options.type}SocksProxy`], parent.options[`${options.type}SocksPort`]))
    }

    if (this.parent.options.tlsAuth) {
      Object.assign(defaultOptions,
        this.applySSL([`${options.type}-cert`, `${options.type}-key`, `${options.type}-pass`, `${options.type}-ca`]))

      Object.assign(defaultOptions, this.applySSL(['cert', 'key', 'pass', 'ca']))
    }

    this.baseRequest = request.defaults(_.pickBy(defaultOptions, _.identity))
  }

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
}

module.exports = Base
