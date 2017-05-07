'use strict'

/*
 * Test suite for transforming documents during copying
 */
var baseUrl = 'http://127.0.0.1:9200'
var indexes = ['source_index', 'destination_index']
var ids = [1, 2]
var request = require('request')
var should = require('should')
var path = require('path')
var async = require('async')
var Elasticdump = require(path.join(__dirname, '..', 'elasticdump.js'))

var clear = function (callback) {
  var jobs = []
  indexes.forEach(function (index) {
    jobs.push(function (done) {
      request.del(baseUrl + '/' + index, done)
    })
  })
  async.series(jobs, callback)
}

var setup = function (callback) {
  var jobs = []

  jobs.push(function (done) {
    var url = baseUrl + '/source_index'
    request.put(url, {body: JSON.stringify({mappings: {test: {}}})}, done)
  })
  ids.forEach(function (i) {
    jobs.push(function (done) {
      var url = baseUrl + '/source_index/test/' + i
      var payload = JSON.stringify({foo: i})
      request.put(url, {body: payload}, done)
    })
  })

  jobs.push(function (done) {
    setTimeout(done, 6000)
  })

  async.series(jobs, callback)
}

describe('multiple transform scripts should be executed for written documents', function () {
  before(function (done) {
    this.timeout(1000 * 20)
    clear(function (error) {
      if (error) { return done(error) }
      setup(function (error) {
        if (error) { return done(error) }
        var jobs = []

        var dataOptions = {
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

        var dataDumper = new Elasticdump(dataOptions.input, dataOptions.output, dataOptions)

        dataDumper.on('error', function (error) { throw (error) })

        jobs.push(function (next) { dataDumper.dump(next) })
        jobs.push(function (next) { setTimeout(next, 5001) })

        async.series(jobs, done)
      })
    })
  })

  after(function (done) { clear(done) })

  it('documents should have the new field computed by both transform scripts', function (done) {
    var url = baseUrl + '/destination_index/_search'
    request.get(url, function (err, response, body) {
      should.not.exist(err)
      body = JSON.parse(body)
      body.hits.total.should.equal(2)
      body.hits.hits.forEach(function (doc) {
        doc._source.bar.should.equal(doc._source.foo * 2)
        doc._source.baz.should.equal(doc._source.bar + 3)
      })
      done()
    })
  })
})
