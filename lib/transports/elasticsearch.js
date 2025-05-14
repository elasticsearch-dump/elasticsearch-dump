const semver = require('semver')
const Many = require('extends-classes')
const request = require('requestretry')
const parseBaseURL = require('../parse-base-url')
const _ = require('lodash')

// classes
const {
  Alias,
  Analyzer,
  Base,
  Data,
  Mapping,
  Policy,
  Setting,
  Template,
  Script
} = require('./__es__')

class elasticsearch extends Many(Base, Alias, Analyzer, Mapping, Policy, Setting, Template, Script, Data) {
  constructor (parent, url, options) {
    super()
    this.base = parseBaseURL(url, options)
    this.options = options
    this.parent = parent
    this.type = parent.options.type
    this.lastScrollId = null
    this.lastSearchAfter = null
    this.settingsExclude = [
      'settings.index.version',
      'settings.index.creation_date',
      'settings.index.uuid',
      'settings.index.provided_name',
      'settings.index.resize',
      'settings.index.routing.allocation.initial_recovery'
    ]
    /**
     * Note that _parent, has been deprecated since ES 6.0
     * Note that _timestamp & _ttl have been deprecated since ES 5.0
     */
    this.defaultMetaFields = ['routing', 'parent', 'timestamp', 'ttl']
    this.totalSearchResults = 0
    this.elementsToSkip = 0
    this.searchBody = this.parent.options.searchBody
    /**
     * Major elastic version.
     *
     * For alternative distributions (OpenSearch), this will be the version they claims
     * to be compatible with.
     *
     * @type {string}
     */
    this.ESversion = null
    /**
     * Full elastic version.
     *
     * For alternative distributions (OpenSearch), this will be the version they claims
     * to be compatible with.
     *
     * @type {string}
     */
    this.ESFullversion = null
    /**
     * Elastic distribution, or flavor.
     *
     * Use `.startsWith(...)` if the check does not care the target is serverless or not.
     *
     * @type {'elastic' | 'elastic-serverless' | 'opensearch' | 'opensearch-serverless'}
     */
    this.ESDistribution = null
    /**
     * Distribution version.
     *
     * For elastic and elastic-serverless distributions, this is identical to ESVersion.
     *
     * For other distributions, this is the actual version the server is running.
     *
     * @type {string}
     */
    this.DistributionVersion = null

    this.IsOpenSearch = false
    this.featureFlag = false

    const defaultOptions = {
      timeout: this.parent.options.timeout,
      gzip: this.parent.options.esCompress,
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

    if (parent.options[`${options.type}-headers`]) {
      Object.assign(defaultOptions.headers, parent.options[`${options.type}-headers`])
    }

    if (options.type && !this.__urlContainsBasicAuth()) {
      const username = process.env[`ELASTICDUMP_${options.type.toUpperCase()}_USERNAME`]
      const password = process.env[`ELASTICDUMP_${options.type.toUpperCase()}_PASSWORD`]
      if (username && password) {
        defaultOptions.auth = { username, password }
      }
    }

    this.baseRequest = request.defaults(_.pickBy(defaultOptions, _.identity))
  }

  __urlContainsBasicAuth () {
    const parsedUrl = new URL(this.base.url)
    return parsedUrl.username !== '' && parsedUrl.password !== ''
  }

  // accept callback
  // return (error, arr) where arr is an array of objects
  get (limit, offset, callback) {
    const type = this.parent.options.type
    this.version('input', err => {
      if (err) { return callback(err) }

      if (type === 'data') {
        this.getData(limit, offset, callback)
      } else if (type === 'mapping') {
        this.getMapping(limit, offset, callback)
      } else if (type === 'analyzer' || type === 'settings' || type === 'index') {
        this.getSettings(limit, offset, callback)
      } else if (type === 'alias') {
        this.getAliases(limit, offset, callback)
      } else if (type === 'template') {
        this.getTemplates(limit, offset, callback)
      } else if (type === 'component_template' || type === 'index_template') {
        if (semver.gte(this.ESFullversion, '7.8.0')) {
          this.featureFlag = true
          this.getTemplates(limit, offset, callback)
        } else {
          callback(new Error(`feature not supported in Elasticsearch ${this.ESFullversion}, only version 7.8.0 or higher`), null)
        }
      } else if (type === 'policy') {
        if (semver.gte(this.ESFullversion, '6.6.0')) {
          this.featureFlag = true
          this.getPolicies(limit, offset, callback)
        } else {
          callback(new Error(`feature not supported in Elasticsearch ${this.ESFullversion}, only version 6.6.0 or higher`), null)
        }
      } else if (type === 'script') {
        if (semver.gte(this.ESFullversion, '7.10.0')) {
          this.getScripts(limit, offset, callback)
        } else {
          callback(new Error(`feature not supported in Elasticsearch ${this.ESFullversion}, only version 7.10.0 or higher`), null)
        }
      } else {
        callback(new Error('unknown type option'), null)
      }
    })
  }

  // accept arr, callback where arr is an array of objects
  // return (error, writes)
  set (data, limit, offset, callback) {
    const type = this.parent.options.type
    this.version('output', err => {
      if (err) { return callback(err) }
      if (type === 'data') {
        this.setData(data, limit, offset, callback)
      } else if (type === 'mapping') {
        this.setMapping(data, limit, offset, callback)
      } else if (type === 'analyzer') {
        this.setAnalyzer(data, limit, offset, callback)
      } else if (type === 'settings' || type === 'index') {
        this.setSettings(data, limit, offset, callback)
      } else if (type === 'alias') {
        this.setAliases(data, limit, offset, callback)
      } else if (type === 'template') {
        this.setTemplates(data, limit, offset, callback)
      } else if (type === 'component_template' || type === 'index_template') {
        if (semver.gte(this.ESFullversion, '7.8.0')) {
          this.setTemplates(data, limit, offset, callback)
        } else {
          callback(new Error(`feature not supported in Elasticsearch ${this.ESFullversion}, only version 7.8.0 or higher`), null)
        }
      } else if (type === 'policy') {
        if (semver.gte(this.ESFullversion, '6.6.0')) {
          this.setPolicies(data, limit, offset, callback)
        } else {
          callback(new Error(`feature not supported in Elasticsearch ${this.ESFullversion}, only version 6.6.0 or higher`), null)
        }
      } else if (type === 'script') {
        if (semver.gte(this.ESFullversion, '7.10.0')) {
          this.setScripts(data, limit, offset, callback)
        } else {
          callback(new Error(`feature not supported in Elasticsearch ${this.ESFullversion}, only version 7.10.0 or higher`), null)
        }
      } else {
        callback(new Error('unknown type option'), null)
      }
    })
  }

  __call (method, args) {
    console.log(`'${method}()' is missing!`)
  }
}

module.exports = {
  elasticsearch
}
