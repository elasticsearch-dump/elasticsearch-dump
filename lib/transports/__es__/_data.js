const zlib = require('zlib')
const jsonParser = require('../../jsonparser.js')
const aws4signer = require('../../aws4signer')
const { parseMetaFields } = require('../../parse-meta-data')
const _ = require('lodash')
const { scrollResultSet, safeDecodeURIComponent, searchAfterResultSet } = require('./_helpers')

class Data {
  async _getDataPit (searchBody, callback) {
    if (!this.lastSearchAfter) {
      // Initialize PIT if enabled
      if (this.parent.options.pit) {
        const pitRequest = {
          uri: `${this.base.url}/_pit?keep_alive=${this.parent.options.pitKeepAlive || '5m'}`,
          method: 'POST'
        }

        try {
          await aws4signer(pitRequest, this.parent)
          const response = await new Promise((resolve, reject) => {
            this.baseRequest(pitRequest, (err, resp) => {
              err = this.handleError(err, resp)
              if (err) {
                return callback(err, [])
              } else resolve(resp)
            })
          })
          const parsed = jsonParser.parse(response.body, this.parent)
          this.pitId = parsed.id
        } catch (err) {
          return callback(err, [])
        }
      }

      searchBody.sort = searchBody.sort || ['_shard_doc']
      if (this.pitId) {
        searchBody.pit = { id: this.pitId }
      }
    }

    return searchAfterResultSet(this, callback)
  }

  /**
   * Search via size/from, not scroll API.
   *
   * This is for OpenSearch Serverless where it does not (yet) support _search?scroll API.
   * See https://docs.aws.amazon.com/opensearch-service/latest/developerguide/serverless-genref.html#serverless-operations.
   */
  async _getDataOffsetLimit (limit, offset, searchBody, callback) {
    searchBody.size = limit
    searchBody.from = this.currentOffset ? this.currentOffset : offset

    const additionalParams = this.paramsToString(this.parent.options[`${this.options.type}-params`] || this.parent.options.params, '&')

    const uri = `${this.base.url}/_search${additionalParams}`

    const searchRequest = {
      uri,
      method: 'GET',
      body: jsonParser.stringify(searchBody)
    }
    aws4signer(searchRequest, this.parent).then((res) => {
      this.baseRequest(searchRequest, (err, response) => {
        err = this.handleError(err, response)
        if (err) {
          return callback(err, [])
        }

        const body = jsonParser.parse(response.body, this.parent)
        const hits = _.get(body, 'hits.hits', [])

        this.currentOffset = offset + limit
        return callback(null, hits)
      })
    }).catch(callback)
  }

  async _getDataScroll (limit, offset, searchBody, callback) {
    // this allows dumps to be resumed of failed pre-maturely
    // ensure scrollTime is set to a fair amount to prevent
    // stream closure
    if (this.parent.options.scrollId && this.lastScrollId === null) {
      this.lastScrollId = this.parent.options.scrollId
    }

    if (this.lastScrollId !== null) {
      this.parent.emit('debug', `lastScrollId: ${this.lastScrollId}`)
      scrollResultSet(this, callback)
      return
    }

    // previously we used the scan/scroll method, but now we need to change the sort
    // https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking_50_search_changes.html#_literal_search_type_scan_literal_removed

    // if this is the first time we run, we need to log how many elements we should be skipping
    if (!this.elementsToSkip) { this.elementsToSkip = offset }

    const additionalParams = this.paramsToString(this.parent.options[`${this.options.type}-params`] || this.parent.options.params, '&')

    // https://www.elastic.co/guide/en/elasticsearch/reference/6.0/breaking_60_search_changes.html#_scroll
    // The from parameter can no longer be used in the search request body when initiating a scroll.
    // The parameter was already ignored in these situations, now in addition an error is thrown.
    let uri = `${this.base.url}/_search?scroll=${this.parent.options.scrollTime}&from=${offset}${additionalParams}`

    searchBody.size = this.parent.options.size >= 0 && this.parent.options.size < limit ? this.parent.options.size : limit

    let searchRequest = {
      uri,
      method: this.parent.options['scroll-with-post'] ? 'POST' : 'GET',
      sort: ['_doc'],
      body: jsonParser.stringify(searchBody)
    }
    aws4signer(searchRequest, this.parent).then(() => {
      this.baseRequest(searchRequest, (err, response) => {
        err = this.handleError(err, response)
        if (err) {
          return callback(err, [])
        }

        const body = jsonParser.parse(response.body, this.parent)
        this.lastScrollId = body._scroll_id

        if (this.lastScrollId === undefined) {
          err = new Error('Unable to obtain scrollId; This tends to indicate an error with your index(es)')
          return callback(err, [])
        } else {
          this.parent.emit('debug', `lastScrollId: ${this.lastScrollId}`)
        }

        // hits.total is now an object in the search response
        // https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html#_literal_hits_total_literal_is_now_an_object_in_the_search_response
        const hitsTotal = _.get(body, 'hits.total.value', body.hits.total)
        this.totalSearchResults = this.parent.options.size >= 0 ? this.parent.options.size : hitsTotal
        this.parent.emit('debug', `Total Search Results: ${this.totalSearchResults}`)

        scrollResultSet(this, callback, body.hits.hits, response)
      })
    }).catch(callback)
  }

  async getData (limit, offset, callback) {
    const searchBody = await this.searchWithTemplate(this.searchBody)

    if (offset >= this.totalSearchResults && this.totalSearchResults !== 0) {
      callback(null, [])
      return
    }

    // Use search_after/pit if specified in options
    if (this.parent.options.searchAfter) {
      return this._getDataPit(searchBody, callback)
    }

    if (this.ESDistribution === "opensearch-serverless") {
      return this._getDataOffsetLimit(limit, offset, searchBody, callback)
    }

    return _getDataScroll(limit, offset, searchBody, callback)
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

    const additionalParams = this.paramsToString(this.parent.options[`${this.options.type}-params`] || this.parent.options.params)

    const thisUrl = `${this.base.url}/_bulk${additionalParams}`
    // Note: OpenSearch Serverless does not support PUT _bulk API, instead POST _bulk API should be used
    // List of supported endpoints: https://docs.aws.amazon.com/opensearch-service/latest/developerguide/serverless-genref.html#serverless-operations
    const payload = {
      url: thisUrl,
      body: '',
      method: this.ESDistribution === "opensearch-serverless" ? 'POST' : 'PUT',
      headers: Object.assign({
        'User-Agent': 'elasticdump',
        'Content-Type': 'application/x-ndjson'
      }, this.parent.options.headers)
    }

    // default is passed here for testing
    const bulkAction = this.parent.options.bulkAction || 'index'

    data.forEach(elem => {
      if (this.ESDistribution === "opensearch-serverless") {
        delete elem._id
        delete elem.fields
      }
      const actionMeta = { [bulkAction]: {} }

      // use index from base otherwise fallback to elem
      actionMeta[bulkAction]._index = safeDecodeURIComponent(this.base.index) || elem._index

      // https://www.elastic.co/guide/en/elasticsearch/reference/master/removal-of-types.html
      if (this.ESversion < 7) {
        // use type from base otherwise fallback to elem
        actionMeta[bulkAction]._type = this.base.type || elem._type
      }
      actionMeta[bulkAction]._id = elem._id

      if (this.parent.options.handleVersion) {
        if (elem.version || elem._version) {
          actionMeta[bulkAction].version = elem.version || elem._version
        }

        if (this.parent.options.versionType) {
          actionMeta[bulkAction].version_type = this.parent.options.versionType
        }
      }

      parseMetaFields(extraFields, [elem, elem.fields], actionMeta, bulkAction)

      payload.body += `${jsonParser.stringify(actionMeta, this.parent)}
`
      payload.body += `${jsonParser.stringify(bulkAction === 'update' ? { doc: elem._source } : elem._source, this.parent)}
`
    })

    this.parent.emit('debug', `thisUrl: ${thisUrl}, payload.body: ${jsonParser.stringify(payload.body, this.parent)}`)

    // overriding the content-encoding
    // https://github.com/elasticsearch-dump/elasticsearch-dump/issues/920#issuecomment-1268390506
    if (this.parent.options.esCompress) {
      payload.headers['Content-Encoding'] = 'gzip'
      payload.body = zlib.gzipSync(payload.body)
    }

    aws4signer(payload, this.parent).then(() => {
      this.baseRequest(payload, (err, response) => {
        err = this.handleError(err, response)
        if (err) {
          return callback(err, [])
        }

        try {
          const r = jsonParser.parse(response.body, this.parent)
          if (r.items !== null && r.items !== undefined) {
            if (r.ok === true) {
              writes = data.length
            } else {
              r.items.forEach(item => {
                if (item[bulkAction].status < 400) {
                  writes++
                } else if (this.parent.options['ignore-es-write-errors']) {
                  console.error(item[bulkAction])
                } else {
                  return callback(item[bulkAction])
                }
              })
            }
          }
        } catch (e) { return callback(e) }

        this.reindex(err => callback(err, writes))
      })
    }).catch(callback)
  }

  del (elem, callback) {
    let thisUrl = `${this.base.host}/${encodeURIComponent(elem._index)}/${encodeURIComponent(elem._type || '_doc')}/${encodeURIComponent(elem._id)}`

    if (this.parent.options['delete-with-routing']) {
      const obj = {}

      _.chain(elem)
        .pick(['routing', '_routing'])
        .each(route => {
          obj.routing = route
          return false
        })
        .value()

      if (Object.keys(obj).length > 0) {
        const additionalParams = this.paramsToString(obj)
        thisUrl += additionalParams
      }
    }

    this.parent.emit('debug', `deleteUrl: ${thisUrl}`)
    const esRequest = {
      url: thisUrl,
      method: 'DELETE'
    }
    aws4signer(esRequest, this.parent).then(() => {
      this.baseRequest(esRequest, (err, response, body) => {
        if (typeof callback === 'function') {
          callback(err, response, body)
        }
      })
    }).catch(callback)
  }

  reindex (callback) {
    if (this.parent.options.noRefresh) {
      callback()
    } else {
      const esRequest = {
        url: `${this.base.url}/_refresh`,
        method: 'POST'
      }
      aws4signer(esRequest, this.parent).then(() => {
        this.baseRequest(esRequest, (err, response) => {
          callback(err, response)
        })
      }).catch(callback)
    }
  }
}

module.exports = Data
