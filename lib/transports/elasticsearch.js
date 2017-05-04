var request = require('request')
var jsonParser = require('../jsonparser.js')
var parseBaseURL = require('../parse-base-url')
var aws4signer = require('../aws4signer')
var async = require('async')

var elasticsearch = function (parent, url, options) {
  this.base = parseBaseURL(url, options.index)
  this.parent = parent
  this.lastScrollId = null
  this.totalSearchResults = 0
  this.elementsToSkip = 0
  this.searchBody = this.parent.options.searchBody
  this.ESversion = null
  this.baseRequest = request.defaults({
    headers: Object.assign({
      'User-Agent': 'elasticdump'
    }, options.headers)
  })
}
// accept callback
// return (error, arr) where arr is an array of objects
elasticsearch.prototype.get = function (limit, offset, callback) {
  var self = this
  var type = self.parent.options.type
  self.version('input', function (err) {
    if (err) { return callback(err) }

    if (type === 'data') {
      self.getData(limit, offset, callback)
    } else if (type === 'mapping') {
      self.getMapping(limit, offset, callback)
    } else if (type === 'analyzer') {
      self.getSettings(limit, offset, callback)
    } else {
      callback(new Error('unknown type option'), null)
    }
  })
}

elasticsearch.prototype.version = function (prefix, callback) {
  var self = this

  if (self.ESversion) { return callback() }
  var esRequest = {
    'url': self.base.host + '/',
    'method': 'GET'
  }
  aws4signer(esRequest, self.parent)
  self.baseRequest.get(esRequest, function (err, response) {
    if (err) { return callback(err) }
    response = JSON.parse(response.body)

    if (response.version) {
      self.ESversion = response.version.number.split('.')[0]
      self.parent.emit('debug', 'discovered elasticsearch ' + prefix + ' major version: ' + self.ESversion)
    } else {
      self.ESversion = 5
      self.parent.emit('debug', 'cannot discover elasticsearch ' + prefix + ' major version, assuming: ' + self.ESversion)
    }

    if (!self.searchBody) {
      if (self.ESversion >= 5) {
        self.searchBody = { 'query': { 'match_all': {} }, 'stored_fields': ['*'], '_source': true }
      } else {
        self.searchBody = { 'query': { 'match_all': {} }, 'fields': ['*'], '_source': true }
      }
    }

    callback()
  })
}

elasticsearch.prototype.getMapping = function (limit, offset, callback) {
  var self = this
  if (self.gotMapping === true) {
    callback(null, [])
  } else {
    var esRequest = {
      'url': self.base.url + '/_mapping',
      'method': 'GET'
    }
    aws4signer(esRequest, self.parent)

    self.baseRequest.get(esRequest, function (err, response) {
      self.gotMapping = true
      var payload = []
      if (!err) {
        response = payload.push(JSON.parse(response.body))
      }
      callback(err, payload)
    })
  }
}

elasticsearch.prototype.getSettings = function (limit, offset, callback) {
  var self = this
  if (self.gotSettings === true) {
    callback(null, [])
  } else {
    var esRequest = {
      'url': self.base.url + '/_settings',
      'method': 'GET'
    }
    aws4signer(esRequest, self.parent)

    self.baseRequest.get(esRequest, function (err, response) {
      self.gotSettings = true
      var payload = []
      if (!err) {
        response = payload.push(response.body)
      }
      callback(err, payload)
    })
  }
}

elasticsearch.prototype.getData = function (limit, offset, callback) {
  var searchRequest, self, uri
  self = this
  var searchBody = self.searchBody

  if (offset >= self.totalSearchResults && self.totalSearchResults !== 0) {
    callback(null, [])
    return
  }

  if (self.lastScrollId !== null) {
    scrollResultSet(self, callback)
  } else {
    // previously we used the scan/scroll method, but now we need to change the sort
    // https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking_50_search_changes.html#_literal_search_type_scan_literal_removed

    // if this is the first time we run, we need to log how many elements we should be skipping
    if (!self.elementsToSkip) { self.elementsToSkip = offset }

    uri = self.base.url +
      '/' +
      '_search?scroll=' +
      self.parent.options.scrollTime +
      '&from=' + offset

    searchBody.size = limit

    searchRequest = {
      'uri': uri,
      'method': 'GET',
      'sort': [ '_doc' ],
      'body': JSON.stringify(searchBody)
    }
    aws4signer(searchRequest, self.parent)

    self.baseRequest.get(searchRequest, function requestResonse (err, response) {
      if (err) {
        callback(err, [])
        return
      } else if (response.statusCode !== 200) {
        err = new Error(response.body)
        callback(err, [])
        return
      }

      var body = jsonParser.parse(response.body)
      self.lastScrollId = body._scroll_id

      if (self.lastScrollId === undefined) {
        err = new Error('Unable to obtain scrollId; This tends to indicate an error with your index(es)')
        callback(err, [])
        return
      }
      self.totalSearchResults = body.hits.total

      scrollResultSet(self, callback, body.hits.hits)
    })
  }
}

// accept arr, callback where arr is an array of objects
// return (error, writes)
elasticsearch.prototype.set = function (data, limit, offset, callback) {
  var self = this
  var type = self.parent.options.type
  self.version('output', function (err) {
    if (err) { return callback(err) }

    if (type === 'data') {
      self.setData(data, limit, offset, callback)
    } else if (type === 'mapping') {
      self.setMapping(data, limit, offset, callback)
    } else if (type === 'analyzer') {
      self.setAnalyzer(data, limit, offset, callback)
    } else {
      callback(new Error('unknown type option'), null)
    }
  })
}

elasticsearch.prototype.setMapping = function (data, limit, offset, callback) {
  var self = this
  if (self.haveSetMapping === true) {
    callback(null, 0)
  } else {
    var esRequest = {
      'url': self.base.url,
      'method': 'PUT'
    }
    aws4signer(esRequest, self.parent)

    self.baseRequest.put(esRequest, function (err, response) { // ensure the index exists
      if (err) { return callback(err) }

      try {
        data = data[0]
      } catch (e) {
        return callback(e)
      }
      var started = 0
      var count = 0
      for (var index in data) {
        var mappings = data[index]['mappings']
        var sortedMappings = []

        for (var key in mappings) {
          if (mappings[key]._parent) {
            sortedMappings = [{key: key, data: mappings[key]}].concat(sortedMappings)
          } else {
            sortedMappings.push({key: key, data: mappings[key]})
          }
        }

        async.eachSeries(sortedMappings, function (set, done) {
          var url = self.base.url + '/' + encodeURIComponent(set.key) + '/_mapping'
          var payload = {
            url: url,
            method: 'PUT',
            body: JSON.stringify(set.data),
            timeout: self.parent.options.timeout
          }
          aws4signer(payload, self.parent)

          started++
          count++

          self.baseRequest.put(payload, function (err, response) {
            started--
            done(null) // we always call this with no error because this is a dirty hack and we are already handling errors...
            if (!err) {
              var bodyError = jsonParser.parse(response.body).error
              if (bodyError) { err = bodyError }
            }
            if (started === 0) {
              self.haveSetMapping = true
              callback(err, count)
            }
          })
        })
      }
    })
  }
}

elasticsearch.prototype.setAnalyzer = function (data, limit, offset, callback) {
  var self = this
  var updateAnalyzer = function (err, response) {
    if (err) { return callback(err) }

    try {
      data = jsonParser.parse(data[0])
    } catch (e) {
      return callback(e)
    }
    var started = 0
    var count = 0
    for (var index in data) {
      var settings = data[index]['settings']
      for (var key in settings) { // iterate through settings
        var setting = {}
        setting[key] = settings[key]
        var url = self.base.url + '/_settings'
        started++
        count++

        // ignore all other settings other than 'analysis'
        for (var p in setting[key]) { // iterate through index
          if (p !== 'analysis') { // remove everything not 'analysis'
            delete setting[key][p]
          }
        }

        var esRequest = {
          'url': self.base.url + '/_close', // close the index
          'method': 'POST',
          'timeout': self.parent.options.timeout
        }
        aws4signer(esRequest, self.parent)

        self.baseRequest.post(esRequest, function (err, response, body) {
          if (!err) {
            var bodyError = jsonParser.parse(response.body).error
            if (bodyError) {
              err = bodyError
            }
            var payload = {
              url: url,
              method: 'PUT',
              body: JSON.stringify(setting),
              timeout: self.parent.options.timeout
            }
            aws4signer(payload, self.parent)

            self.baseRequest.put(payload, function (err, response) { // upload the analysis settings
              started--
              if (!err) {
                var bodyError = jsonParser.parse(response.body).error
                if (bodyError) {
                  err = bodyError
                }
              } else {
                callback(err, count)
              }
              if (started === 0) {
                self.haveSetAnalyzer = true
                var esRequest = {
                  'url': self.base.url + '/_open', // open the index
                  'method': 'POST',
                  'timeout': self.parent.options.timeout
                }
                aws4signer(esRequest, self.parent)

                self.baseRequest.post(esRequest, function (err, response) {
                  if (!err) {
                    var bodyError = jsonParser.parse(response.body).error
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
  if (self.haveSetAnalyzer === true) {
    callback(null, 0)
  } else {
    var esRequest = {
      'url': self.base.url,
      'method': 'PUT'
    }
    aws4signer(esRequest, self.parent)

    self.baseRequest.put(esRequest, function (err, response) { // ensure the index exists
      if (err) { return callback(err) }

      // use cluster health api to check if the index is ready
      esRequest = {
        'url': self.base.host + '/_cluster/health/' + self.base.index + '?wait_for_status=green',
        'method': 'GET'
      }
      aws4signer(esRequest, self.parent)
      self.baseRequest.get(esRequest, updateAnalyzer)
    })
  }
}

elasticsearch.prototype.setData = function (data, limit, offset, callback) {
  if (data.length === 0) { return callback(null, 0) }

  var self = this
  var extraFields = ['routing', 'parent', 'timestamp', 'ttl']
  var writes = 0

  var thisUrl = self.base.url + '/_bulk'

  var payload = {
    url: thisUrl,
    body: '',
    method: 'PUT',
    timeout: self.parent.options.timeout
  }

  data.forEach(function (elem) {
    var actionMeta = { index: {} }
    // actionMeta._index = elem._index; // Not setting _index should use the one defined in URL.
    actionMeta.index._type = elem._type
    actionMeta.index._id = elem._id

    extraFields.forEach(function (field) {
      if (elem.fields) {
        if (elem.fields[field]) {
          actionMeta.index[field] = elem.fields[field]
        }
        if (elem.fields['_' + field]) {
          actionMeta.index[field] = elem.fields['_' + field]
        }
      } else {
        if (elem[field]) {
          actionMeta.index[field] = elem[field]
        }
        if (elem['_' + field]) {
          actionMeta.index[field] = elem['_' + field]
        }
      }
    })

    payload.body += JSON.stringify(actionMeta) + '\n'
    payload.body += JSON.stringify(elem._source) + '\n'
  })

  self.parent.emit('debug', 'thisUrl: ' + thisUrl + ', payload.body: ' + JSON.stringify(payload.body))

  aws4signer(payload, self.parent)
  self.baseRequest.put(payload, function (err, response) {
    if (err) { return callback(err) }

    try {
      var r = jsonParser.parse(response.body)
      if (r.items !== null && r.items !== undefined) {
        if (r.ok === true) {
          writes = data.length
        } else {
          r.items.forEach(function (item) {
            if (item['index'].status < 400) {
              writes++
            } else {
              console.error(item['index'])
            }
          })
        }
      }
    } catch (e) { return callback(e) }

    self.reindex(function (err) {
      return callback(err, writes)
    })
  })
}

elasticsearch.prototype.del = function (elem, callback) {
  var self = this
  var thisUrl = self.base.url + '/' + encodeURIComponent(elem._type) + '/' + encodeURIComponent(elem._id)

  self.parent.emit('debug', 'deleteUrl: ' + thisUrl)
  var esRequest = {
    'url': thisUrl,
    'method': 'DELETE'
  }
  aws4signer(esRequest, self.parent)

  self.baseRequest.del(esRequest, function (err, response, body) {
    if (typeof callback === 'function') {
      callback(err, response, body)
    }
  })
}

elasticsearch.prototype.reindex = function (callback) {
  var self = this
  if (self.parent.options.noRefresh) {
    callback()
  } else {
    var esRequest = {
      'url': self.base.url + '/_refresh',
      'method': 'POST'
    }
    aws4signer(esRequest, self.parent)

    self.baseRequest.post(esRequest, function (err, response) {
      callback(err, response)
    })
  }
}

exports.elasticsearch = elasticsearch

// ///////////
// HELPERS //
// ///////////

/**
 * Posts requests to the _search api to fetch the latest
 * scan result with scroll id
 * @param self
 * @param callback
 */
function scrollResultSet (self, callback, loadedHits) {
  var body

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
        var started = 0
        loadedHits.forEach(function (elem) {
          started++
          self.del(elem, function () {
            started--
            if (started === 0) {
              self.reindex(function (err) {
                return callback(err, loadedHits)
              })
            }
          })
        })
      } else {
        return callback(null, loadedHits)
      }
    } else {
      return scrollResultSet(self, callback)
    }
  } else {
    var scrollRequest = {
      'uri': self.base.host + '/_search' + '/scroll?scroll=' + self.parent.options.scrollTime,
      'method': 'GET',
      'body': self.lastScrollId
    }
    aws4signer(scrollRequest, self.parent)

    self.baseRequest.get(scrollRequest, function requestResonse (err, response) {
      if (err) {
        callback(err, [])
        return
      }

      self.parent.emit('debug', 'scrollRequest: ' + JSON.stringify(scrollRequest))
      self.parent.emit('debug', 'body: ' + JSON.stringify(response.body))

      if (err === null && response.statusCode !== 200) {
        err = new Error(response.body)
        callback(err, [])
        return
      }

      try {
        body = jsonParser.parse(response.body)
      } catch (e) {
        e.message = e.message + ' | Cannot Parse: ' + response.body
        callback(e, [])
        return
      }

      self.lastScrollId = body._scroll_id
      var hits = body.hits.hits

      if (self.parent.options.delete === true && hits.length > 0) {
        var started = 0
        hits.forEach(function (elem) {
          started++
          self.del(elem, function () {
            started--
            if (started === 0) {
              self.reindex(function (err) {
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
