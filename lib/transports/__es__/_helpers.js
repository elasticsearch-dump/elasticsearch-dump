const jsonParser = require('../../jsonparser.js')
const aws4signer = require('../../aws4signer')
const delay = require('delay')
const _ = require('lodash')

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

    if (self.ESversion === '1') {
      // body based parameters were added in 2.0.0
      // scroll_id needs to be sent raw (base64 encoded)
      Object.assign(scrollRequest, {
        uri: `${scrollRequest.uri}?scroll=${self.parent.options.scrollTime}`,
        body: self.lastScrollId
      })
    } else if (awsChain || awsAccessKeyId || awsIniFileProfile) {
      Object.assign(scrollRequest, {
        uri: `${scrollRequest.uri}?scroll=${self.parent.options.scrollTime}`,
        body: jsonParser.stringify({
          scroll_id: self.lastScrollId
        }),
        method: 'GET'
      })
    } else {
      Object.assign(scrollRequest, {
        body: jsonParser.stringify({
          scroll: self.parent.options.scrollTime,
          scroll_id: self.lastScrollId
        })
      })
    }

    aws4signer(scrollRequest, self.parent).then(() => {
      self.baseRequest(scrollRequest, (err, response) => {
        err = self.handleError(err, response)
        if (err) {
          return callback(err, [])
        }

        self.parent.emit('debug', `scrollRequest: ${jsonParser.stringify(scrollRequest)}`)
        self.parent.emit('debug', `body: ${jsonParser.stringify(response.body)}`)

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

        if (body.terminated_early && body._shards && body._shards.failed > 0) {
          return delay(self.parent.options.scrollRetryDelay || 0)
            .then(() => {
              scrollResultSet(self, callback)
            })
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
    }).catch(callback)
  }
}

const searchAfterResultSet = async (self, callback, hits = null, response = null) => {
  let error
  let loadedHits

  if (hits === null) {
    const searchAfterRequest = {
      uri: `${self.base[self.pitId ? 'host' : 'url']}/_search`,
      method: 'POST',
      body: jsonParser.stringify({
        size: self.parent.options.limit,
        ...self.searchBody,
        search_after: self.lastSearchAfter ? self.lastSearchAfter : undefined,
        pit: self.pitId ? { id: self.pitId, keep_alive: self.parent.options.pitKeepAlive || '5m' } : undefined
      })
    }

    try {
      await aws4signer(searchAfterRequest, self.parent)
      response = await new Promise((resolve, reject) => {
        self.baseRequest(searchAfterRequest, (err, resp) => {
          error = self.handleError(err, resp)
          if (error) {
            reject(error)
          } else {
            resolve(resp)
          }
        })
      })

      const parsed = jsonParser.parse(response.body, self.parent)
      if (parsed.pit_id) self.pitId = parsed.pit_id
      loadedHits = parsed.hits.hits
    } catch (err) {
      return callback(err, [])
    }
  } else {
    loadedHits = hits
  }

  if (loadedHits.length === 0) {
    if (self.pitId) {
      await closePit(self)
    }
    return callback(null, [])
  }

  self.lastSearchAfter = loadedHits[loadedHits.length - 1].sort
  return callback(null, loadedHits, response)
}

const closePit = async (self) => {
  if (!self.pitId) return

  const closeRequest = self.IsOpenSearch
    ? {
        uri: `${self.base.url}/_search/point_in_time`,
        method: 'DELETE',
        body: jsonParser.stringify({ pit_id: [self.pitId] })
      }
    : {
        uri: `${self.base.url}/_pit`,
        method: 'DELETE',
        body: jsonParser.stringify({ id: self.pitId })
      }

  try {
    await aws4signer(closeRequest, self.parent)
    await new Promise((resolve, reject) => {
      self.baseRequest(closeRequest, (err, resp) => {
        if (err) reject(err)
        else resolve(resp)
      })
    })
  } catch (err) {
    self.parent.emit('warning', `Error closing PIT: ${err}`)
  }
}

const safeDecodeURIComponent = (uri) => {
  // fixes #1014
  if (_.isNil(uri)) return uri
  try {
    return decodeURIComponent(uri)
  } catch (_) {
    return uri
  }
}

module.exports = {
  scrollResultSet,
  safeDecodeURIComponent,
  searchAfterResultSet
}
