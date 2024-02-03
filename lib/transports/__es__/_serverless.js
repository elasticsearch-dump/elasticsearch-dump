const jsonParser = require('../../jsonparser.js')
const aws4signer = require('../../aws4signer')
const _ = require('lodash')

class Serverless {
  async getOpenSearchServerlessData (limit, offset, callback) {
    const searchBody = await this.searchWithTemplate(this.searchBody)
    searchBody.size = limit
    searchBody.from = this.currentOffset ? this.currentOffset : offset
    const additionalParams = this.paramsToString(this.parent.options[`${this.options.type}-params`] || this.parent.options.params, '&')
    // Note: OpenSearch Serverless does not support _search?scroll API. As a workaround pure _search is used
    // List of supported endpoints: https://docs.aws.amazon.com/opensearch-service/latest/developerguide/serverless-genref.html#serverless-operations
    const uri = `${this.base.url}/_search${additionalParams}`

    const searchRequest = {
      uri,
      method: 'GET',
      body: jsonParser.stringify(searchBody)
    }
    aws4signer(searchRequest, this.parent).then((res) => {
      this.baseRequest(searchRequest, (err, response) => {
        err = this.handleError(err, response)
        if (err) {
          return callback(err, [])
        }

        const body = jsonParser.parse(response.body, this.parent)
        const hits = _.get(body, 'hits.hits', [])

        this.currentOffset = offset + limit
        return callback(null, hits)
      })
    }).catch(callback)
  }
}

module.exports = Serverless
