const request = require('requestretry')
const jsonParser = require('../jsonparser.js')
const parseBaseURL = require('../parse-base-url')
const aws4signer = require('../aws4signer')
const async = require('async')
const _ = require('lodash')
const { parseMetaFields } = require('../parse-meta-data')
const fs = require('fs')
const agent = require('socks5-http-client/lib/Agent')
const sslAgent = require('socks5-https-client/lib/Agent')
const status = require('http-status')

class elasticsearch {
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
        elasticsearch.applyProxy(url, parent.options[`${options.type}SocksProxy`], parent.options[`${options.type}SocksPort`]))
    }

    if (this.parent.options.tlsAuth) {
      Object.assign(defaultOptions,
        this.applySSL([`${options.type}-cert`, `${options.type}-key`, `${options.type}-pass`, `${options.type}-ca`]))

      Object.assign(defaultOptions, this.applySSL(['cert', 'key', 'pass', 'ca']))
    }

    this.baseRequest = request.defaults(_.pickBy(defaultOptions, _.identity))
  }

  static applyProxy (url, proxyHost, proxyPort) {
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
      } else {
        callback(new Error('unknown type option'), null)
      }
    })
  }

  version (prefix, callback) {
    if (this.ESversion) { return callback() }
    const esRequest = {
      url: `${this.base.host}/`,
      method: 'GET'
    }
    aws4signer(esRequest, this.parent)
    this.baseRequest(esRequest, (err, response) => {
      if (err) { return callback(err) }
      response = jsonParser.parse(response.body)

      if (response.version) {
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
  }

  getMapping (limit, offset, callback) {
    if (this.gotMapping === true) {
      callback(null, [])
    } else {
      const esRequest = {
        url: `${this.base.url}/_mapping`,
        method: 'GET'
      }
      aws4signer(esRequest, this.parent)

      this.baseRequest(esRequest, (err, response) => {
        this.gotMapping = true
        const payload = []
        if (!err) {
          response = payload.push(jsonParser.parse(response.body))
        }
        callback(err, payload)
      })
    }
  }

  getSettings (limit, offset, callback) {
    if (this.gotSettings === true) {
      callback(null, [])
    } else {
      const esRequest = {
        url: `${this.base.url}/_settings`,
        method: 'GET'
      }
      aws4signer(esRequest, this.parent)

      this.baseRequest(esRequest, (err, response) => {
        this.gotSettings = true
        const payload = []
        if (!err) {
          const output = jsonParser.parse(response.body)
          output[this.base.index] = _.omit(output[this.base.index], this.settingsExclude)
          payload.push(jsonParser.stringify(output))
        }
        callback(err, payload)
      })
    }
  }

  getAliases (limit, offset, callback) {
    if (this.gotAliases === true) {
      callback(null, [])
    } else {
      const esRequest = {
        url: `${this.base.host}/${this.base.index}/_alias/${this.base.type || '*'}`,
        method: 'GET'
      }
      aws4signer(esRequest, this.parent)

      this.baseRequest(esRequest, (err, response) => {
        this.gotAliases = true
        const payload = []
        if (!err) {
          payload.push(response.body)
        }
        callback(err, payload)
      })
    }
  }

  getTemplates (limit, offset, callback) {
    if (this.gotTemplates === true) {
      callback(null, [])
    } else {
      const esRequest = {
        url: `${this.base.host}/_template/${this.base.index}`,
        method: 'GET'
      }
      aws4signer(esRequest, this.parent)

      this.baseRequest(esRequest, (err, response) => {
        this.gotTemplates = true
        const payload = []
        if (!err) {
          payload.push(response.body)
        }
        callback(err, payload)
      })
    }
  }

  async getData (limit, offset, callback) {
    let searchRequest, uri
    let searchBodyTmp

    if (this.parent.options.searchWithTemplate) {
      searchBodyTmp = await this.renderTemplate(this.searchBody.id, this.searchBody.params)
        .then(result => {
          return result
        })
        .catch(error => {
          throw new Error(error)
        })
    } else {
      searchBodyTmp = this.searchBody
    }

    const searchBody = searchBodyTmp

    if (offset >= this.totalSearchResults && this.totalSearchResults !== 0) {
      callback(null, [])
      return
    }

    // this allows dumps to be resumed of failed pre-maturely
    // ensure scrollTime is set to a fair amount to prevent
    // stream closure
    if (this.parent.options.scrollId && this.lastScrollId === null) {
      this.lastScrollId = this.parent.options.scrollId
    }

    if (this.lastScrollId !== null) {
      this.parent.emit('debug', `lastScrollId: ${this.lastScrollId}`)
      scrollResultSet(this, callback)
    } else {
      // previously we used the scan/scroll method, but now we need to change the sort
      // https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking_50_search_changes.html#_literal_search_type_scan_literal_removed

      // if this is the first time we run, we need to log how many elements we should be skipping
      if (!this.elementsToSkip) { this.elementsToSkip = offset }

      const paramsObj = this.parent.options.params
      const additionalParams = paramsObj
        ? `&${Object.keys(paramsObj).map(key => `${key}=${paramsObj[key]}`).join('&')}`
        : ''

      // https://www.elastic.co/guide/en/elasticsearch/reference/6.0/breaking_60_search_changes.html#_scroll
      // The from parameter can no longer be used in the search request body when initiating a scroll.
      // The parameter was already ignored in these situations, now in addition an error is thrown.
      uri = `${this.base.url}/_search?scroll=${this.parent.options.scrollTime}&from=${offset}${additionalParams}`

      searchBody.size = this.parent.options.size >= 0 && this.parent.options.size < limit ? this.parent.options.size : limit

      searchRequest = {
        uri: uri,
        method: 'GET',
        sort: ['_doc'],
        body: jsonParser.stringify(searchBody)
      }
      aws4signer(searchRequest, this.parent)

      this.baseRequest(searchRequest, (err, response) => {
        if (err) {
          callback(err, [])
          return
        } else if (response.statusCode !== 200) {
          err = new Error(response.body)
          callback(err, [])
          return
        }

        const body = jsonParser.parse(response.body, this.parent)
        this.lastScrollId = body._scroll_id

        if (this.lastScrollId === undefined) {
          err = new Error('Unable to obtain scrollId; This tends to indicate an error with your index(es)')
          callback(err, [])
          return
        } else {
          this.parent.emit('debug', `lastScrollId: ${this.lastScrollId}`)
        }

        // hits.total is now an object in the search response
        // https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html#_literal_hits_total_literal_is_now_an_object_in_the_search_response
        const hitsTotal = _.get(body, 'hits.total.value', body.hits.total)
        this.totalSearchResults = this.parent.options.size >= 0 ? this.parent.options.size : hitsTotal

        scrollResultSet(this, callback, body.hits.hits, response)
      })
    }
  }

  renderTemplate (id, params) {
    const uri = `${this.base.host}/_render/template/${id}`

    const renderTemplateRequestBody = { params: params }

    const renderTemplateRequest = {
      uri: uri,
      method: 'GET',
      body: jsonParser.stringify(renderTemplateRequestBody)
    }

    return new Promise((resolve, reject) => {
      this.baseRequest(renderTemplateRequest, (err, success) => {
        if (!err && success.statusCode === 200) {
          const render = jsonParser.parse(success.body).template_output
          resolve(render)
        } else {
          reject(err)
        }
      })
    }
    )
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
      } else {
        callback(new Error('unknown type option'), null)
      }
    })
  }

  setMapping (data, limit, offset, callback) {
    if (this.haveSetMapping === true || data.length === 0) {
      callback(null, 0)
    } else {
      const esRequest = {
        url: this.base.url,
        method: 'PUT'
      }
      aws4signer(esRequest, this.parent)

      this.baseRequest(esRequest, (err, response) => { // ensure the index exists
        if (err) { return callback(err) }

        try {
          data = data[0]
        } catch (e) {
          return callback(e)
        }
        let started = 0
        let count = 0

        const paramsObj = this.parent.options.params
        const additionalParams = paramsObj
          ? `?${Object.keys(paramsObj).map(key => `${key}=${paramsObj[key]}`).join('&')}`
          : ''

        for (const index in data) {
          const mappings = data[index].mappings
          let sortedMappings = []

          // make sure new mappings inserted before parent and after child
          for (const key in mappings) {
            if (mappings[key]._parent) {
              const parentIndex = sortedMappings.findIndex(set => set.key === mappings[key]._parent.type) // find parent
              if (parentIndex > -1) {
                sortedMappings.splice(parentIndex, 0, { key, index, data: mappings[key] })
              } else {
                const childIndex = sortedMappings.findIndex(set => (set.data._parent) && (set.data._parent.type === key)) // find child
                if (childIndex > -1) {
                  sortedMappings.splice(childIndex + 1, 0, { key, index, data: mappings[key] })
                } else {
                  sortedMappings = [{ key, index, data: mappings[key] }].concat(sortedMappings)
                }
              }
            } else {
              sortedMappings.push({ key, index, data: mappings[key] })
            }
          }

          async.eachSeries(sortedMappings, (set, done) => {
            let __type = ''
            if (this.ESversion < 7) {
              __type = `/${encodeURIComponent(set.key)}`
            } else if (set.key !== 'properties') {
              // handle other mapping properties
              // fixes #667
              set.data = { [set.key]: set.data }
            } else if (!set.data.properties) {
              set.data = { properties: set.data }
            }

            if (!this.base.index) {
              __type = `/${set.index}${__type}`
            }

            const url = `${this.base.url}${__type}/_mapping${additionalParams}`
            const payload = {
              url,
              method: 'PUT',
              body: jsonParser.stringify(set.data)
            }
            aws4signer(payload, this.parent)

            started++
            count++

            this.baseRequest(payload, (err, response) => {
              started--
              done(null) // we always call this with no error because this is a dirty hack and we are already handling errors...
              if (!err) {
                const bodyError = jsonParser.parse(response.body).error
                if (bodyError) { err = bodyError }
              }
              if (started === 0) {
                this.haveSetMapping = true
                callback(err, count)
              }
            })
          })
        }
      })
    }
  }

  setAnalyzer (data, limit, offset, callback) {
    const updateAnalyzer = (err, response) => {
      if (err) { return callback(err) }

      try {
        data = jsonParser.parse(data[0])
      } catch (e) {
        return callback(e)
      }
      let started = 0
      let count = 0
      for (const index in data) {
        const settings = data[index].settings
        for (const key in settings) { // iterate through settings
          const setting = {}
          setting[key] = settings[key]
          const url = `${this.base.url}/_settings`
          started++
          count++

          // ignore all other settings other than 'analysis'
          for (const p in setting[key]) { // iterate through index
            if (p !== 'analysis') { // remove everything not 'analysis'
              delete setting[key][p]
            }
          }

          const esRequest = {
            url: `${this.base.url}/_close`, // close the index
            method: 'POST'
          }
          aws4signer(esRequest, this.parent)

          this.baseRequest(esRequest, (err, response, body) => {
            if (!err) {
              const bodyError = jsonParser.parse(response.body).error
              if (bodyError) {
                err = bodyError
              }
              const payload = {
                url: url,
                method: 'PUT',
                body: jsonParser.stringify(setting)
              }
              aws4signer(payload, this.parent)

              this.baseRequest(payload, (err, response) => { // upload the analysis settings
                started--
                if (!err) {
                  const bodyError = jsonParser.parse(response.body).error
                  if (bodyError) {
                    err = bodyError
                  }
                } else {
                  callback(err, count)
                }
                if (started === 0) {
                  this.haveSetAnalyzer = true
                  const esRequest = {
                    url: `${this.base.url}/_open`, // open the index
                    method: 'POST'
                  }
                  aws4signer(esRequest, this.parent)

                  this.baseRequest(esRequest, (err, response) => {
                    if (!err) {
                      const bodyError = jsonParser.parse(response.body).error
                      if (bodyError) {
                        err = bodyError
                      }
                    }
                    callback(err, count)
                  })
                }
              })
            } else {
              callback(err, count)
            }
          })
        }
      }
    }
    if (this.haveSetAnalyzer === true || data.length === 0) {
      callback(null, 0)
    } else {
      let esRequest = {
        url: this.base.url,
        method: 'PUT'
      }
      aws4signer(esRequest, this.parent)

      this.baseRequest(esRequest, (err, response) => { // ensure the index exists
        if (err) {
          return callback(err, [])
        } else if (response.statusCode !== 200) {
          err = new Error(response.body)
          callback(err, [])
          return
        }

        // use cluster health api to check if the index is ready
        esRequest = {
          url: `${this.base.host}/_cluster/health/${this.base.index}?wait_for_status=green`,
          method: 'GET'
        }
        aws4signer(esRequest, this.parent)
        this.baseRequest(esRequest, updateAnalyzer)
      })
    }
  }

  setSettings (data, limit, offset, callback) {
    if (this.haveSetSettings === true || data.length === 0) {
      return callback(null, 0)
    }

    try {
      data = jsonParser.parse(data[0])
    } catch (e) {
      return callback(e)
    }

    let writes = 0

    async.forEachOf(data, (index, name, cb) => {
      let settings = _.omit(index, this.settingsExclude)
      if (this.ESversion < 7) {
        settings = settings.settings
      }

      const esRequest = {
        url: this.base.url,
        method: 'PUT',
        body: jsonParser.stringify(settings)
      }
      aws4signer(esRequest, this.parent)

      this.baseRequest(esRequest, (err, response) => { // ensure the index exists
        if (err) {
          return cb(err, [])
        } else if (response.statusCode !== 200) {
          err = new Error(response.body)
          callback(err, [])
          return
        }
        writes++
        return cb()
      })
    }, err => {
      if (err) { return callback(err) }
      this.haveSetSettings = true
      return callback(null, writes)
    })
  }

  setAliases (data, limit, offset, callback) {
    if (this.haveSetAliases === true || data.length === 0) {
      return callback(null, 0)
    }

    try {
      data = jsonParser.parse(data[0])
    } catch (e) {
      return callback(e)
    }

    const payload = {
      actions: []
    }

    let writes = 0

    async.forEachOf(data, async.ensureAsync((_data, index, cb) => {
      if (!_.has(_data, 'aliases') || _.isEmpty(_data.aliases)) {
        return cb(new Error('no aliases detected'))
      }

      async.forEachOf(_data.aliases, async.ensureAsync((aliasOptions, alias, acb) => {
        payload.actions.push({ add: Object.assign({ index, alias }, aliasOptions) })
        writes++
        return acb()
      }), () => {
        return cb()
      })
    }), err => {
      if (err) { return callback(err) }
      this.haveSetAliases = true

      const esRequest = {
        url: `${this.base.host}/_aliases`,
        method: 'POST',
        body: jsonParser.stringify(payload)
      }
      aws4signer(esRequest, this.parent)

      this.baseRequest(esRequest, (err, response) => {
        if (err) { return callback(err) }
        return callback(null, writes)
      })
    })
  }

  setTemplates (data, limit, offset, callback) {
    if (this.haveSetTemplates === true || data.length === 0) {
      return callback(null, 0)
    }

    try {
      data = jsonParser.parse(data[0])
    } catch (e) {
      return callback(e)
    }

    let writes = 0

    async.forEachOf(data, async.ensureAsync((_template, templateName, cb) => {
      const esRequest = {
        url: `${this.base.host}/_template/${templateName}`,
        method: 'PUT',
        body: jsonParser.stringify(_template)
      }
      aws4signer(esRequest, this.parent)

      this.baseRequest(esRequest, (err, response) => {
        if (err) { return cb(err) }
        writes++
        return cb()
      })
    }), err => {
      if (err) { return callback(err) }
      this.haveSetTemplates = true
      return callback(null, writes)
    })
  }

  setData (data, limit, offset, callback) {
    if (data.length === 0) { return callback(null, 0) }

    let writes = 0

    const extraFields = _.chain(this.parent.options.parseExtraFields)
      .split(',')
      .concat(this.defaultMetaFields)
      .flatten()
      .compact()
      .uniq()
      .value()

    const paramsObj = this.parent.options.params
    const additionalParams = paramsObj
      ? `?${Object.keys(paramsObj).map(key => `${key}=${paramsObj[key]}`).join('&')}`
      : ''

    const thisUrl = `${this.base.url}/_bulk${additionalParams}`

    const payload = {
      url: thisUrl,
      body: '',
      method: 'PUT',
      headers: Object.assign({
        'User-Agent': 'elasticdump',
        'Content-Type': 'application/x-ndjson'
      }, this.parent.options.headers)
    }

    data.forEach(elem => {
      const actionMeta = { index: {} }

      // use index from base otherwise fallback to elem
      actionMeta.index._index = this.base.index || elem._index

      // https://www.elastic.co/guide/en/elasticsearch/reference/master/removal-of-types.html
      if (this.ESversion < 7) {
        // use type from base otherwise fallback to elem
        actionMeta.index._type = this.base.type || elem._type
      }
      actionMeta.index._id = elem._id

      if (this.parent.options.handleVersion) {
        if (elem.version || elem._version) {
          actionMeta.index.version = elem.version || elem._version
        }

        if (this.parent.options.versionType) {
          actionMeta.index.version_type = this.parent.options.versionType
        }
      }

      parseMetaFields(extraFields, [elem, elem.fields], actionMeta)

      payload.body += `${jsonParser.stringify(actionMeta, this.parent)}
`
      payload.body += `${jsonParser.stringify(elem._source, this.parent)}
`
    })

    this.parent.emit('debug', `thisUrl: ${thisUrl}, payload.body: ${jsonParser.stringify(payload.body, this.parent)}`)

    aws4signer(payload, this.parent)
    this.baseRequest(payload, (err, response) => {
      if (err) {
        callback(err, [])
        return
      } else if (response.statusCode !== 200) {
        err = new Error(response.body)
        err.statusCode = response.statusCode
        err.name = status[`${response.statusCode}_NAME`]
        if (!err.message) {
          err.message = status[`${response.statusCode}_MESSAGE`]
        }
        callback(err, [])
        return
      }

      try {
        const r = jsonParser.parse(response.body, this.parent)
        if (r.items !== null && r.items !== undefined) {
          if (r.ok === true) {
            writes = data.length
          } else {
            r.items.forEach(item => {
              if (item.index.status < 400) {
                writes++
              } else {
                console.error(item.index)
              }
            })
          }
        }
      } catch (e) { return callback(e) }

      this.reindex(err => callback(err, writes))
    })
  }

  del (elem, callback) {
    const thisUrl = `${this.base.host}/${encodeURIComponent(elem._index)}/${encodeURIComponent(elem._type)}/${encodeURIComponent(elem._id)}`

    this.parent.emit('debug', `deleteUrl: ${thisUrl}`)
    const esRequest = {
      url: thisUrl,
      method: 'DELETE'
    }
    aws4signer(esRequest, this.parent)

    this.baseRequest(esRequest, (err, response, body) => {
      if (typeof callback === 'function') {
        callback(err, response, body)
      }
    })
  }

  reindex (callback) {
    if (this.parent.options.noRefresh) {
      callback()
    } else {
      const esRequest = {
        url: `${this.base.url}/_refresh`,
        method: 'POST'
      }
      aws4signer(esRequest, this.parent)

      this.baseRequest(esRequest, (err, response) => {
        callback(err, response)
      })
    }
  }
}

module.exports = {
  elasticsearch
}

// ///////////
// HELPERS //
// ///////////

/**
 * Posts requests to the _search api to fetch the latest
 * scan result with scroll id
 * @param self
 * @param callback
 * @param loadedHits
 * @param response
 */
const scrollResultSet = (self, callback, loadedHits, response) => {
  let body

  if (loadedHits && loadedHits.length > 0) {
    // are we skipping and we have hits?
    if (self.elementsToSkip > 0) {
      while (loadedHits.length > 0 && self.elementsToSkip > 0) {
        loadedHits.splice(0, 1)
        self.elementsToSkip--
      }
    }

    if (loadedHits.length > 0) {
      if (self.parent.options.delete === true) {
        let started = 0
        loadedHits.forEach(elem => {
          started++
          self.del(elem, () => {
            started--
            if (started === 0) {
              self.reindex(err => callback(err, loadedHits, response))
            }
          })
        })
      } else {
        return callback(null, loadedHits, response)
      }
    } else {
      return scrollResultSet(self, callback)
    }
  } else {
    const scrollRequest = {
      uri: `${self.base.host}/_search/scroll`,
      method: 'POST'
    }

    const { awsChain, awsAccessKeyId, awsIniFileProfile } = self.parent.options

    if (awsChain || awsAccessKeyId || awsIniFileProfile) {
      Object.assign(scrollRequest, {
        uri: `${scrollRequest.uri}?scroll=${self.parent.options.scrollTime}`,
        body: jsonParser.stringify({
          scroll_id: self.lastScrollId
        }),
        method: 'GET'
      })
    } else if (self.ESversion === '1') {
      // body based parameters were added in 2.0.0
      // scroll_id needs to be sent raw (base64 encoded)
      Object.assign(scrollRequest, {
        uri: `${scrollRequest.uri}?scroll=${self.parent.options.scrollTime}`,
        body: self.lastScrollId
      })
    } else {
      Object.assign(scrollRequest, {
        body: jsonParser.stringify({
          scroll: self.parent.options.scrollTime,
          scroll_id: self.lastScrollId
        })
      })
    }

    aws4signer(scrollRequest, self.parent)

    self.baseRequest(scrollRequest, (err, response) => {
      if (err) {
        callback(err, [])
        return
      }

      self.parent.emit('debug', `scrollRequest: ${jsonParser.stringify(scrollRequest)}`)
      self.parent.emit('debug', `body: ${jsonParser.stringify(response.body)}`)

      if (err === null && response.statusCode !== 200) {
        err = new Error(response.body)
        callback(err, [])
        return
      }

      try {
        body = jsonParser.parse(response.body, self.parent)
      } catch (e) {
        e.message = `${e.message} | Cannot Parse: ${response.body}`
        callback(e, [])
        return
      }

      self.lastScrollId = body._scroll_id
      const hits = body.hits.hits

      if (self.lastScrollId) {
        self.parent.emit('debug', `lastScrollId: ${self.lastScrollId}`)
      }

      if (self.parent.options.delete === true && hits.length > 0) {
        let started = 0
        hits.forEach(elem => {
          started++
          self.del(elem, () => {
            started--
            if (started === 0) {
              self.reindex(err => {
                if (hits.length === 0) {
                  self.lastScrollId = null
                }
                callback(err, hits)
              })
            }
          })
        })
      } else {
        if (hits.length === 0) {
          self.lastScrollId = null
        }

        // are we skipping and we have hits?
        if (self.elementsToSkip > 0 && hits.length > 0) {
          while (hits.length > 0 && self.elementsToSkip > 0) {
            hits.splice(0, 1)
            self.elementsToSkip--
          }

          if (hits.length > 0) {
            // we have some hits after skipping, lets callback
            return callback(err, hits)
          } else {
            // we skipped, but now we don't have any hits,
            // scroll again for more data if possible
            return scrollResultSet(self, callback)
          }
        } else {
          // not skipping or done skipping
          return callback(err, hits)
        }
      }
    })
  }
}
