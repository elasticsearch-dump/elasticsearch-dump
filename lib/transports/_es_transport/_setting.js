const jsonParser = require('../../jsonparser.js')
const aws4signer = require('../../aws4signer')
const async = require('async')
const _ = require('lodash')

class Setting {
  getSettings (limit, offset, callback) {
    if (this.gotSettings === true) {
      callback(null, [])
    } else {
      const esRequest = {
        url: `${this.base.url}/_settings`,
        method: 'GET'
      }
      aws4signer(esRequest, this.parent).then(() => {
        this.baseRequest(esRequest, (err, response) => {
          this.gotSettings = true
          const payload = []
          err = this.handleError(err, response)
          if (err) {
            return callback(err, [])
          }

          const output = jsonParser.parse(response.body)
          output[this.base.index] = _.omit(output[this.base.index], this.settingsExclude)
          payload.push(jsonParser.stringify(output))

          callback(err, payload)
        })
      }).catch(callback)
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
      if (_.isEmpty(index) || _.isEmpty(name)) return cb()

      let settings = _.omit(index, this.settingsExclude)
      if (this.ESversion < 7) {
        settings = settings.settings
      }

      const esRequest = {
        url: this.base.url,
        method: 'PUT',
        body: jsonParser.stringify(settings)
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
      this.haveSetSettings = true
      return callback(null, writes)
    })
  }
}

module.exports = Setting
