const jsonParser = require('../jsonparser.js')
const aws4signer = require('../aws4signer')
const status = require('http-status')
const semver = require('semver')
const Many = require('extends-classes')

// classes
const Base = require('./_es_transport/base')
const Alias = require('./_es_transport/_alias')
const Analyzer = require('./_es_transport/_analyzer')
const Mapping = require('./_es_transport/_mapping')
const Setting = require('./_es_transport/_setting')
const Template = require('./_es_transport/_template')
const Data = require('./_es_transport/data')

class elasticsearch extends Many(Base, Alias, Analyzer, Mapping, Setting, Template, Data) {
  version (prefix, callback) {
    if (this.ESversion) { return callback() }
    const esRequest = {
      url: `${this.base.host}/`,
      method: 'GET'
    }
    aws4signer(esRequest, this.parent).then(() => {
      this.baseRequest(esRequest, (err, response) => {
        if (err) {
          return callback(err, [])
        } else if (response.statusCode !== 200) {
          err = new Error(response.body)
          err.statusCode = response.statusCode
          err.name = status[`${response.statusCode}_NAME`]
          if (!err.message) {
            err.message = status[`${response.statusCode}_MESSAGE`]
          }
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
      } else if (type === 'analyzer' || type === 'settings') {
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
      } else if (type === 'settings') {
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
