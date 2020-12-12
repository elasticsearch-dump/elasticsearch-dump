const jsonParser = require('../../jsonparser.js')
const aws4signer = require('../../aws4signer')

class Analyzer {
  setAnalyzer (data, limit, offset, callback) {
    const updateAnalyzer = (err, response) => {
      if (err) { return callback(err) }

      try {
        data = jsonParser.parse(data[0])
      } catch (e) {
        return callback(e)
      }
      let started = 0
      let count = 0
      for (const index in data) {
        const settings = data[index].settings
        for (const key in settings) { // iterate through settings
          const setting = {}
          setting[key] = settings[key]
          const url = `${this.base.url}/_settings`
          started++
          count++

          // ignore all other settings other than 'analysis'
          for (const p in setting[key]) { // iterate through index
            if (p !== 'analysis') { // remove everything not 'analysis'
              delete setting[key][p]
            }
          }

          const esRequest = {
            url: `${this.base.url}/_close`, // close the index
            method: 'POST'
          }
          aws4signer(esRequest, this.parent).then(() => {
            this.baseRequest(esRequest, (err, response, body) => {
              if (!err) {
                const bodyError = jsonParser.parse(response.body).error
                if (bodyError) {
                  err = bodyError
                }
                const payload = {
                  url: url,
                  method: 'PUT',
                  body: jsonParser.stringify(setting)
                }
                aws4signer(payload, this.parent).then(() => {
                  this.baseRequest(payload, (err, response) => { // upload the analysis settings
                    started--
                    if (!err) {
                      const bodyError = jsonParser.parse(response.body).error
                      if (bodyError) {
                        err = bodyError
                      }
                    } else {
                      callback(err, count)
                    }
                    if (started === 0) {
                      this.haveSetAnalyzer = true
                      const esRequest = {
                        url: `${this.base.url}/_open`, // open the index
                        method: 'POST'
                      }
                      aws4signer(esRequest, this.parent).then(() => {
                        this.baseRequest(esRequest, (err, response) => {
                          if (!err) {
                            const bodyError = jsonParser.parse(response.body).error
                            if (bodyError) {
                              err = bodyError
                            }
                          }
                          callback(err, count)
                        })
                      })
                    }
                  })
                })
              } else {
                callback(err, count)
              }
            })
          }).catch(callback)
        }
      }
    }
    if (this.haveSetAnalyzer === true || data.length === 0) {
      callback(null, 0)
    } else {
      let esRequest = {
        url: this.base.url,
        method: 'PUT'
      }
      aws4signer(esRequest, this.parent).then(() => {
        this.baseRequest(esRequest, (err, response) => { // ensure the index exists
          if (err) {
            return callback(err, [])
          } else if (response.statusCode !== 200) {
            err = new Error(response.body)
            callback(err, [])
            return
          }

          // use cluster health api to check if the index is ready
          esRequest = {
            url: `${this.base.host}/_cluster/health/${this.base.index}?wait_for_status=green`,
            method: 'GET'
          }
          aws4signer(esRequest, this.parent).then(() => {
            this.baseRequest(esRequest, updateAnalyzer)
          })
        })
      }).catch(callback)
    }
  }
}

module.exports = Analyzer
