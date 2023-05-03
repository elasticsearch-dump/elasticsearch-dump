const jsonParser = require('../../jsonparser.js')
const aws4signer = require('../../aws4signer')
const async = require('async')
const _ = require('lodash')

class Index {
  getIndex (limit, offset, callback) {
    if (this.gotIndex === true) {
      callback(null, [])
    } else {
      const esRequest = {
        url: `${this.base.url}/`,
        method: 'GET'
      }
      aws4signer(esRequest, this.parent).then(() => {
        this.baseRequest(esRequest, (err, response) => {
          this.gotIndex = true
          const payload = []

          err = this.handleError(err, response)
          if (err) {
            return callback(err, [])
          }

          payload.push(jsonParser.parse(response.body))
          callback(err, payload)
        })
      }).catch(callback)
    }
  }

  setIndex (data, limit, offset, callback) {
    if (this.haveSetIndex === true || data.length === 0) {
      return callback(null, 0)
    }

    try {
      data = data[0]
    } catch (e) {
      return callback(e)
    }

    let writes = 0

    async.forEachOf(data, (index, name, cb) => {
      if (_.isEmpty(index) || _.isEmpty(name)) return cb()

      index = _.omit(index, this.settingsExclude)

      const esRequest = {
        url: this.base.url,
        method: 'PUT',
        body: jsonParser.stringify(index)
      }
      aws4signer(esRequest, this.parent).then(() => {
        this.baseRequest(esRequest, (err, response) => { // ensure the index exists
          err = this.handleError(err, response)
          if (err) {
            return cb(err, [])
          }
          writes++
          return cb()
        })
      }).catch(cb)
    }, err => {
      if (err) { return callback(err) }
      this.haveSetIndex = true
      return callback(null, writes)
    })
  }
}

module.exports = Index
