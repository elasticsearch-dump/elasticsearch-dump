const jsonParser = require('../../jsonparser.js')
const aws4signer = require('../../aws4signer')
const async = require('async')

class Mapping {
  getMapping (limit, offset, callback) {
    if (this.gotMapping === true) {
      callback(null, [])
    } else {
      const esRequest = {
        url: `${this.base.url}/_mapping`,
        method: 'GET'
      }
      aws4signer(esRequest, this.parent).then(() => {
        this.baseRequest(esRequest, (err, response) => {
          this.gotMapping = true
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

  setMapping (data, limit, offset, callback) {
    if (this.haveSetMapping === true || data.length === 0) {
      callback(null, 0)
    } else {
      const esRequest = {
        url: this.base.url,
        method: 'PUT'
      }
      aws4signer(esRequest, this.parent).then(() => {
        this.baseRequest(esRequest, (err, response) => { // ensure the index exists
          if (err) { return callback(err) }

          try {
            data = data[0]
          } catch (e) {
            return callback(e)
          }
          let started = 0
          let count = 0

          const additionalParams = this.paramsToString(this.parent.options.params)

          for (const index in data) {
            const mappings = data[index].mappings
            let sortedMappings = []

            // make sure new mappings inserted before parent and after child
            for (const key in mappings) {
              if (mappings[key]._parent) {
                const parentIndex = sortedMappings.findIndex(set => set.key === mappings[key]._parent.type) // find parent
                if (parentIndex > -1) {
                  sortedMappings.splice(parentIndex, 0, { key, index, data: mappings[key] })
                } else {
                  const childIndex = sortedMappings.findIndex(set => (set.data._parent) && (set.data._parent.type === key)) // find child
                  if (childIndex > -1) {
                    sortedMappings.splice(childIndex + 1, 0, { key, index, data: mappings[key] })
                  } else {
                    sortedMappings = [{ key, index, data: mappings[key] }].concat(sortedMappings)
                  }
                }
              } else {
                sortedMappings.push({ key, index, data: mappings[key] })
              }
            }

            async.eachSeries(sortedMappings, (set, done) => {
              let __type = ''
              if (this.ESversion < 7) {
                __type = `/${encodeURIComponent(set.key)}`
              } else if (set.key !== 'properties') {
                // handle other mapping properties
                // fixes #667
                set.data = { [set.key]: set.data }
              } else if (!set.data.properties) {
                set.data = { properties: set.data }
              }

              if (!this.base.index) {
                __type = `/${set.index}${__type}`
              }

              const url = `${this.base.url}${__type}/_mapping${additionalParams}`
              const payload = {
                url,
                method: 'PUT',
                body: jsonParser.stringify(set.data)
              }
              started++
              count++

              aws4signer(payload, this.parent).then(() => {
                this.baseRequest(payload, (err, response) => {
                  started--
                  done(null) // we always call this with no error because this is a dirty hack and we are already handling errors...
                  if (!err) {
                    const bodyError = jsonParser.parse(response.body).error
                    if (bodyError) { err = bodyError }
                  }
                  if (started === 0) {
                    this.haveSetMapping = true
                    callback(err, count)
                  }
                })
              })
            })
          }
        })
      }).catch(callback)
    }
  }
}

module.exports = Mapping
