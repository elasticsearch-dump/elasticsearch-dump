const jsonParser = require('../../jsonparser.js')
const aws4signer = require('../../aws4signer')
const { parseMetaFields } = require('../../parse-meta-data')
const _ = require('lodash')
const { scrollResultSet } = require('./_helpers')

class Data {
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
    }).catch(callback)
  }

  del (elem, callback) {
    const thisUrl = `${this.base.host}/${encodeURIComponent(elem._index)}/${encodeURIComponent(elem._type)}/${encodeURIComponent(elem._id)}`

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
