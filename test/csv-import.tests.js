const http = require('http')
http.globalAgent.maxSockets = 10

const path = require('path')
const Elasticdump = require(path.join(__dirname, '..', 'elasticdump.js'))
const should = require('should')
const baseUrl = 'http://127.0.0.1:9200'
const _ = require('lodash')
const seedSize = 500
const testTimeout = seedSize * 25

const request = require('request').defaults({
  headers: {
    'User-Agent': 'elasticdump',
    'Content-Type': 'application/json'
  }
})

const loadTemplate = (templateName, templateBody, callback) => {
  const payload = { url: baseUrl + '/_template/' + templateName, body: JSON.stringify(templateBody) }
  request.put(payload, (err, response) => { // create the index first with potential custom analyzers before seeding
    should.not.exist(err)
    callback()
  })
}

const clear = callback => {
  request.del(baseUrl + '/cars_index', (err, response, body) => {
    should.not.exist(err)
    callback()
  })
}

const getTotal = (body) => _.get(body, 'hits.total.value', body.hits.total)

describe('csv import', () => {
  beforeEach(function (done) {
    this.timeout(testTimeout)
    clear(() => {
      const isNew = /[6-9]\.\d+\..+/.test(process.env.ES_VERSION)

      const templateSettings = {
        [isNew ? 'index_patterns' : 'template']: isNew ? ['cars_index'] : '*_index',
        settings: {
          number_of_shards: 1,
          number_of_replicas: 0
        }
      }

      // settings for index to be created with
      loadTemplate('template_1xxx', templateSettings, () => {
        done()
      })
    })
  })

  it('can connect', function (done) {
    this.timeout(testTimeout)
    request(baseUrl, (err, response, body) => {
      should.not.exist(err)
      body = JSON.parse(body)
      body.tagline.should.equal('You Know, for Search')
      done()
    })
  })

  describe('csv file to es', () => {
    it('works', function (done) {
      this.timeout(testTimeout * 2)
      const options = {
        limit: 100,
        offset: 0,
        debug: false,
        type: 'data',
        input: `csv://${path.join(__dirname, 'test-resources', 'cars.csv')}`,
        output: `${baseUrl}/cars_index/cars`,
        csvFirstRowAsHeaders: true,
        csvDelimiter: ';',
        csvSkipLines: 0,
        csvSkipRows: 1,
        csvMaxRows: 0
      }

      const dumper = new Elasticdump(options.input, options.output, options)

      dumper.dump(() => {
        const url = `${baseUrl}/cars_index`
        request.post(`${url}/_refresh`, (err, response) => {
          should.not.exist(err)
          request.get(`${url}/_search`, (err, response, sourceBody) => {
            should.not.exist(err)
            sourceBody = JSON.parse(sourceBody)
            getTotal(sourceBody).should.be.above(0)
            done()
          })
        })
      })
    })
  })
})
