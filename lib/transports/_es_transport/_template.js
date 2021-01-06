const jsonParser = require('../../jsonparser.js')
const aws4signer = require('../../aws4signer')
const async = require('async')
const _ = require('lodash')

class Template {
  getTemplates (limit, offset, callback) {
    if (this.gotTemplates === true) {
      callback(null, [])
    } else {
      const esRequest = {
        url: `${this.base.host}/_${this.parent.options.type}/${this.base.index || '*'}`,
        method: 'GET'
      }
      aws4signer(esRequest, this.parent).then(() => {
        this.baseRequest(esRequest, (err, response) => {
          this.gotTemplates = true
          err = this.handleError(err, response)
          if (err) {
            return callback(err, [])
          }

          if (this.parent.options.filterSystemTemplates) {
            if (this.featureFlag) {
              const template = jsonParser.parse(response.body)
              const key = Object.keys(template)[0]
              if (key) {
                const result = _.reject(template[key], it => new RegExp(this.parent.options.templateRegex).test(it.name))
                return callback(null, [jsonParser.stringify({ [key]: result })])
              }
            } else {
              const templates = jsonParser.parse(response.body)
              const result = _.omitBy(templates, (v, k) => new RegExp(this.parent.options.templateRegex).test(k))
              return callback(null, [jsonParser.stringify(result)])
            }
          }
          callback(null, [response.body])
        })
      }).catch(callback)
    }
  }

  renderTemplate (id, params) {
    const uri = `${this.base.host}/_render/template/${id}`

    const renderTemplateRequestBody = { params: params }

    const renderTemplateRequest = {
      uri: uri,
      method: 'GET',
      body: jsonParser.stringify(renderTemplateRequestBody)
    }

    return new Promise((resolve, reject) => {
      this.baseRequest(renderTemplateRequest, (err, response) => {
        err = this.handleError(err, response)
        if (err) {
          return reject(err)
        }

        const render = jsonParser.parse(response.body).template_output
        resolve(render)
      })
    }
    )
  }

  setTemplates (data, limit, offset, callback) {
    if (this.haveSetTemplates === true || data.length === 0) {
      return callback(null, 0)
    }

    try {
      data = jsonParser.parse(data[0])
    } catch (e) {
      return callback(e)
    }

    let writes = 0

    async.forEachOf(data, async.ensureAsync((_template, templateName, cb) => {
      const esRequest = {
        url: `${this.base.host}/_${this.parent.options.type}/${templateName}`,
        method: 'PUT',
        body: jsonParser.stringify(_template)
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
      this.haveSetTemplates = true
      return callback(null, writes)
    })
  }
}

module.exports = Template
