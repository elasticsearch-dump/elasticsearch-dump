const http = require('http')
http.globalAgent.maxSockets = 10

const path = require('path')
const Elasticdump = require(path.join(__dirname, '..', 'elasticdump.js'))
const should = require('should')
const baseUrl = 'http://127.0.0.1:9200'
const _ = require('lodash')
const testTimeout = 3000

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
      this.timeout(testTimeout)
      const options = {
        limit: 100,
        offset: 0,
        debug: false,
        type: 'data',
        input: `csv://${path.join(__dirname, 'test-resources', 'cars.csv')}`,
        output: baseUrl + '/cars_index',
        csvFirstRowAsHeaders: true,
        csvDelimiter: ';',
        csvSkipLines: 0,
        csvSkipRows: 1
      }

      const dumper = new Elasticdump(options.input, options.output, options)

      dumper.dump(() => {
        const url = baseUrl + '/cars_index/_search'
        request.get(url, (err, response, body) => {
          should.not.exist(err)
          body = JSON.parse(body)
          getTotal(body).should.equal(406)
          done()
        })
      })
    })
  })
})
