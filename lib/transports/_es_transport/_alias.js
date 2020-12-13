const jsonParser = require('../../jsonparser.js')
const aws4signer = require('../../aws4signer')
const async = require('async')
const _ = require('lodash')

class Alias {
  getAliases (limit, offset, callback) {
    if (this.gotAliases === true) {
      callback(null, [])
    } else {
      const esRequest = {
        url: `${this.base.host}/${this.base.index}/_alias/${this.base.type || '*'}`,
        method: 'GET'
      }
      aws4signer(esRequest, this.parent).then(() => {
        this.baseRequest(esRequest, (err, response) => {
          this.gotAliases = true
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

module.exports = Alias
