const jsonParser = require('../../jsonparser.js')
const aws4signer = require('../../aws4signer')
const async = require('async')
const _ = require('lodash')

class Script {
  getScripts (limit, offset, callback) {
    if (this.gotScripts === true) {
      callback(null, [])
    } else {
      const esRequest = {
        url: `${this.base.host}/${this.base.index}/_scripts/${this.base.type || '*'}`,
        method: 'GET'
      }
      aws4signer(esRequest, this.parent).then(() => {
        this.baseRequest(esRequest, (err, response) => {
          this.gotScripts = true
          const payload = []
          err = this.handleError(err, response)
          if (err) {
            return callback(err, [])
          }
          payload.push(response.body)
          callback(err, payload)
        })
      }).catch(callback)
    }
  }

  setScripts (data, limit, offset, callback) {
    if (this.haveSetScripts === true || data.length === 0) {
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
      if (!_.has(_data, 'scritps') || _.isEmpty(_data.aliases)) {
        return cb(new Error('no scripts detected'))
      }

      async.forEachOf(_data.scripts, async.ensureAsync((scriptOptions, alias, acb) => {
        payload.actions.push({ add: Object.assign({ index, alias }, scriptOptions) })
        writes++
        return acb()
      }), () => {
        return cb()
      })
    }), err => {
      if (err) { return callback(err) }
      this.haveSetScripts = true

      const esRequest = {
        url: `${this.base.host}/_scripts`,
        method: 'POST',
        body: jsonParser.stringify(payload)
      }
      aws4signer(esRequest, this.parent).then(() => {
        this.baseRequest(esRequest, (err, response) => {
          err = this.handleError(err, response)
          if (err) {
            return callback(err)
          }
          return callback(null, writes)
        })
      }).catch(callback)
    })
  }
}

module.exports = Script
