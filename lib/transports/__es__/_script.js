const jsonParser = require('../../jsonparser.js')
const aws4signer = require('../../aws4signer')
const async = require('async')
const _ = require('lodash')

class Script {
  _transformMeta (body) {
    const _path = 'metadata.stored_scripts'
    const data = jsonParser.parse(body)
    const result = { stored_scripts: [] }
    if (_.has(data, _path)) {
      // Object.keys(_.get(data, _path)).forEach()
      const res = _.reduce(_.get(data, _path), (result, value, key) => {
        result.push({
          _id: key,
          found: true,
          script: value
        })
        return result
      }, [])

      result.stored_scripts = res
    }

    return jsonParser.stringify(result)
  }

  getScripts (limit, offset, callback) {
    if (this.gotScripts === true) {
      callback(null, [])
    } else {
      const esRequest = {
        url: `${this.base.host}/_cluster/state/metadata?filter_path=**.stored_scripts.${this.base.index || '*'}`,
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
          payload.push(this._transformMeta(response.body))
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

    let writes = 0

    async.each(data.stored_scripts, async.ensureAsync((_script, cb) => {
      const esRequest = {
        url: `${this.base.host}/_scripts/${_script._id}`,
        method: 'POST',
        body: jsonParser.stringify(_.pick(_script, 'script'))
      }
      aws4signer(esRequest, this.parent).then(() => {
        this.baseRequest(esRequest, (err, response) => {
          err = this.handleError(err, response)
          if (err) {
            return cb(err, [])
          }
          writes++
          return cb()
        })
      }).catch(cb)
    }), err => {
      if (err) { return callback(err) }
      this.haveSetScripts = true
      return callback(null, writes)
    })
  }
}

module.exports = Script
