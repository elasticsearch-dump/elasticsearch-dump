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
      done()
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

  describe('file to es', () => {
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
        csvSkipRows: 1
      }

      const dumper = new Elasticdump(options.input, options.output, options)

      dumper.dump(() => {
        const url = `${baseUrl}/cars_index`
        request.post(`${url}/_refresh`, (err, response) => {
          should.not.exist(err)
          request.get(`${url}/_search`, (err, response, sourceBody) => {
            should.not.exist(err)
            sourceBody = JSON.parse(sourceBody)
            getTotal(sourceBody).should.equal(406)
            done()
          })
        })
      })
    })
  })
})
