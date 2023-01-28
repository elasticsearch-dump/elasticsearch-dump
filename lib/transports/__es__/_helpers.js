const jsonParser = require('../../jsonparser.js')
const aws4signer = require('../../aws4signer')
const delay = require('delay')

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

module.exports = {
  scrollResultSet
}
