const jsonParser = require('../../jsonparser.js')
const aws4signer = require('../../aws4signer')
const async = require('async')

class Policy {
  getPolicies (limit, offset, callback) {
    if (this.gotPolicies === true) {
      callback(null, [])
    } else {
      const esRequest = {
        url: `${this.base.host}/_ilm/policy/${this.base.index || ''}`,
        method: 'GET'
      }
      aws4signer(esRequest, this.parent).then(() => {
        this.baseRequest(esRequest, (err, response) => {
          this.gotPolicies = true
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

  setPolicies (data, limit, offset, callback) {
    if (this.haveSetPolicies === true || data.length === 0) {
      return callback(null, 0)
    }

    try {
      data = jsonParser.parse(data[0])
    } catch (e) {
      return callback(e)
    }

    let writes = 0

    async.forEachOf(data, async.ensureAsync((_policy, policyName, cb) => {
      const esRequest = {
        url: `${this.base.host}/_ilm/policy/${policyName}`,
        method: 'PUT',
        body: jsonParser.stringify({ policy: _policy.policy })
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
      this.haveSetPolicies = true
      return callback(null, writes)
    })
  }
}

module.exports = Policy
