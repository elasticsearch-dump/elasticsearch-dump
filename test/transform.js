'use strict'

/*
 * Test suite for transforming documents during copying
 */
const baseUrl = 'http://127.0.0.1:9200'
const indexes = ['source_index', 'destination_index']
const ids = [1, 2]
const request = require('request')
const should = require('should')
const path = require('path')
const async = require('async')
const crypto = require('crypto')
const Elasticdump = require(path.join(__dirname, '..', 'elasticdump.js'))
const _ = require('lodash')
const headers = {'Content-Type': 'application/json'}

const clear = callback => {
  const jobs = []
  indexes.forEach(index => {
    jobs.push(done => {
      request.del(baseUrl + '/' + index, done)
    })
  })
  async.series(jobs, callback)
}

const setup = callback => {
  const jobs = []

  jobs.push(done => {
    const url = baseUrl + '/source_index'
    request.put(url, {body: JSON.stringify({mappings: {test: {}}}), headers}, done)
  })
  ids.forEach(i => {
    jobs.push(done => {
      const url = baseUrl + '/source_index/test/' + i
      const payload = JSON.stringify({foo: i})
      request.put(url, {body: payload, headers}, done)
    })
  })

  jobs.push(done => {
    setTimeout(done, 6000)
  })

  async.series(jobs, callback)
}

const getTotal = (body) => _.get(body, 'hits.total.value', body.hits.total)

describe('multiple transform scripts should be executed for written documents', () => {
  before(function (done) {
    this.timeout(1000 * 20)
    clear(error => {
      if (error) { return done(error) }
      setup(error => {
        if (error) { return done(error) }
        const jobs = []

        const dataOptions = {
          limit: 100,
          offset: 0,
          debug: true,
          type: 'data',
          input: baseUrl + '/source_index',
          output: baseUrl + '/destination_index',
          scrollTime: '10m',
          transform: [
            'doc._source["bar"] = doc._source.foo * 2',
            'doc._source["baz"] = doc._source.bar + 3'
          ]
        }

        const dataDumper = new Elasticdump(dataOptions.input, dataOptions.output, dataOptions)

        dataDumper.on('error', error => { throw (error) })

        jobs.push(next => { dataDumper.dump(next) })
        jobs.push(next => { setTimeout(next, 5001) })

        async.series(jobs, done)
      })
    })
  })

  after(done => { clear(done) })

  it('documents should have the new field computed by both transform scripts', done => {
    const url = baseUrl + '/destination_index/_search'
    request.get(url, (err, response, body) => {
      should.not.exist(err)
      body = JSON.parse(body)
      getTotal(body).should.equal(2)
      body.hits.hits.forEach(doc => {
        doc._source.bar.should.equal(doc._source.foo * 2)
        doc._source.baz.should.equal(doc._source.bar + 3)
      })
      done()
    })
  })
})

describe('external transform module should be executed for written documents', () => {
  before(function (done) {
    this.timeout(1000 * 20)
    clear(error => {
      if (error) { return done(error) }
      setup(error => {
        if (error) { return done(error) }
        const jobs = []

        const dataOptions = {
          limit: 100,
          offset: 0,
          debug: true,
          type: 'data',
          input: baseUrl + '/source_index',
          output: baseUrl + '/destination_index',
          scrollTime: '10m',
          transform: '@./test/test-resources/transform'
        }

        const dataDumper = new Elasticdump(dataOptions.input, dataOptions.output, dataOptions)

        dataDumper.on('error', error => { throw (error) })

        jobs.push(next => { dataDumper.dump(next) })
        jobs.push(next => { setTimeout(next, 5001) })

        async.series(jobs, done)
      })
    })
  })

  after(done => { clear(done) })

  it('documents should have the new field computed by external transform module', done => {
    const url = baseUrl + '/destination_index/_search'
    request.get(url, (err, response, body) => {
      should.not.exist(err)
      body = JSON.parse(body)
      getTotal(body).should.equal(2)
      body.hits.hits.forEach(doc => {
        doc._source.bar.should.equal(
          crypto
            .createHash('md5')
            .update(String(doc._source.foo))
            .digest('hex')
        )
      })
      done()
    })
  })
})
