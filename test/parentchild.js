'use strict'

/* Test suite for parent-child relationships
 *
 * Parent-Child relations demand a mapping, otherwise the indexation
 * is rejected with an error.
 */

var path = require('path')
var Elasticdump = require(path.join(__dirname, '..', 'elasticdump.js'))
var request = require('request')
var should = require('should')
var fs = require('fs')
var async = require('async')
var baseUrl = 'http://127.0.0.1:9200'
var indexes = ['source_index', 'destination_index', 'file_destination_index', 'another_index']
var files = ['/tmp/mapping.json', '/tmp/data.json']
var cities = ['new_york', 'san_francisco', 'london', 'tokyo']
var people = ['evan', 'christina', 'pablo', 'brian', 'aaron']
var mapping = {
  city: {},
  person: { _parent: { type: 'city' } }
}

var clear = function (callback) {
  var jobs = []
  indexes.forEach(function (index) {
    jobs.push(function (done) {
      request.del(baseUrl + '/' + index, done)
    })
  })

  files.forEach(function (file) {
    jobs.push(function (done) {
      try {
        fs.unlinkSync(file)
      } catch (e) { }
      done()
    })
  })

  async.series(jobs, callback)
}

var setup = function (callback) {
  var jobs = []

  jobs.push(function (done) {
    var url = baseUrl + '/source_index'
    var payload = {mappings: mapping}
    request.put(url, {body: JSON.stringify(payload)}, done)
  })

  cities.forEach(function (city) {
    jobs.push(function (done) {
      var url = baseUrl + '/source_index/city/' + city
      var payload = {name: city}
      request.put(url, {body: JSON.stringify(payload)}, done)
    })

    people.forEach(function (person) {
      jobs.push(function (done) {
        var url = baseUrl + '/source_index/person/' + person + '_' + city + '?parent=' + city
        var payload = {name: person, city: city}
        request.put(url, {body: JSON.stringify(payload)}, done)
      })
    })
  })

  jobs.push(function (done) {
    setTimeout(done, 6000)
  })

  async.series(jobs, callback)
}

describe('parent child', function () {
  before(function (done) {
    this.timeout(15 * 1000)
    clear(function (error) {
      if (error) { return done(error) }
      setup(done)
    })
  })

  after(clear)

  it('did the setup properly and parents + children are loaded', function (done) {
    var url = baseUrl + '/source_index/_search'
    request.get(url, function (err, response, body) {
      should.not.exist(err)
      body = JSON.parse(body)
      // this confirms that there are no orphans too!
      body.hits.total.should.equal(cities.length + (cities.length * people.length))
      done()
    })
  })

  describe('each city should have children', function () {
    cities.forEach(function (city) {
      it(city + ' should have children', function (done) {
        var url = baseUrl + '/source_index/_search'
        var payload = {
          'query': {
            'has_parent': {
              'parent_type': 'city',
              'query': {
                'wildcard': {
                  'name': ''
                }
              }
            }
          }
        }
        payload.query.has_parent.query.wildcard.name = '*' + city + '*'

        request.get(url, {body: JSON.stringify(payload)}, function (err, response, body) {
          should.not.exist(err)
          body = JSON.parse(body)
          body.hits.total.should.equal(people.length)
          done()
        })
      })
    })
  })

  describe('ES to ES dump should maintain parent-child relationships', function () {
    before(function (done) {
      this.timeout(2000 * 10)
      var jobs = []

      var mappingOptions = {
        limit: 100,
        offset: 0,
        debug: false,
        type: 'mapping',
        input: baseUrl + '/source_index',
        output: baseUrl + '/destination_index',
        scrollTime: '10m'
      }

      var dataOptions = {
        limit: 100,
        offset: 0,
        debug: false,
        type: 'data',
        input: baseUrl + '/source_index',
        output: baseUrl + '/destination_index',
        scrollTime: '10m'
      }

      var mappingDumper = new Elasticdump(mappingOptions.input, mappingOptions.output, mappingOptions)
      var dataDumper = new Elasticdump(dataOptions.input, dataOptions.output, dataOptions)

      mappingDumper.on('error', function (error) { throw (error) })
      dataDumper.on('error', function (error) { throw (error) })

      jobs.push(function (next) { mappingDumper.dump(next) })
      jobs.push(function (next) { setTimeout(next, 5001) })
      jobs.push(function (next) { dataDumper.dump(next) })
      jobs.push(function (next) { setTimeout(next, 5001) })

      async.series(jobs, done)
    })

    it('the dump transfered', function (done) {
      var url = baseUrl + '/destination_index/_search'
      request.get(url, function (err, response, body) {
        should.not.exist(err)
        body = JSON.parse(body)
        // this confirms that there are no orphans too!
        body.hits.total.should.equal(cities.length + (cities.length * people.length))
        done()
      })
    })

    describe('each city should have children', function () {
      cities.forEach(function (city) {
        it(city + ' should have children', function (done) {
          var url = baseUrl + '/destination_index/_search'
          var payload = {
            'query': {
              'has_parent': {
                'parent_type': 'city',
                'query': {
                  'wildcard': {
                    'name': ''
                  }
                }
              }
            }
          }
          payload.query.has_parent.query.wildcard.name = '*' + city + '*'

          request.get(url, {body: JSON.stringify(payload)}, function (err, response, body) {
            should.not.exist(err)
            body = JSON.parse(body)
            body.hits.total.should.equal(people.length)
            done()
          })
        })
      })
    })
  })

  describe('ES to File and back to ES should work', function () {
    before(function (done) {
      this.timeout(2000 * 10)
      var jobs = []

      var mappingOptions = {
        limit: 100,
        offset: 0,
        debug: false,
        type: 'mapping',
        input: baseUrl + '/source_index',
        output: '/tmp/mapping.json',
        scrollTime: '10m'
      }

      var dataOptions = {
        limit: 100,
        offset: 0,
        debug: false,
        type: 'data',
        input: baseUrl + '/source_index',
        output: '/tmp/data.json',
        scrollTime: '10m'
      }

      var mappingDumper = new Elasticdump(mappingOptions.input, mappingOptions.output, mappingOptions)
      var dataDumper = new Elasticdump(dataOptions.input, dataOptions.output, dataOptions)

      mappingDumper.on('error', function (error) { throw (error) })
      dataDumper.on('error', function (error) { throw (error) })

      jobs.push(function (next) { mappingDumper.dump(next) })
      jobs.push(function (next) { setTimeout(next, 5001) })
      jobs.push(function (next) { dataDumper.dump(next) })
      jobs.push(function (next) { setTimeout(next, 5001) })

      async.series(jobs, done)
    })

    it('the dump files should have worked', function (done) {
      var mapping = String(fs.readFileSync('/tmp/mapping.json'))
      var data = String(fs.readFileSync('/tmp/data.json'))
      var mappingLines = []
      var dataLines = []

      mapping.split('\n').forEach(function (line) {
        if (line.length > 2) { mappingLines.push(JSON.parse(line)) }
      })
      data.split('\n').forEach(function (line) {
        if (line.length > 2) { dataLines.push(JSON.parse(line)) }
      })

      mappingLines.length.should.equal(1)
      Object.keys(mappingLines[0].source_index.mappings).length.should.equal(2)
      should.not.exist(mappingLines[0].source_index.mappings.city._parent)
      mappingLines[0].source_index.mappings.person._parent.type.should.equal('city')

      var dumpedPeople = []
      var dumpedCties = []
      dataLines.forEach(function (d) {
        if (d._type === 'person') {
          var parent
          if (d._parent) { parent = d._parent }  // ES 2.x
          if (d.fields && d.fields._parent) { parent = d.fields._parent }  // ES 1.x
          should.exist(parent)
          dumpedPeople.push(d)
        }
        if (d._type === 'city') {
          should.not.exist(d._parent)
          dumpedCties.push(d)
        }
      })

      dumpedPeople.length.should.equal(cities.length * people.length)
      dumpedCties.length.should.equal(cities.length)

      done()
    })

    describe('can restore from a dumpfile', function () {
      before(function (done) {
        this.timeout(2000 * 10)
        var jobs = []

        var mappingOptions = {
          limit: 100,
          offset: 0,
          debug: true,
          type: 'mapping',
          input: '/tmp/mapping.json',
          output: baseUrl + '/file_destination_index'
        }

        var dataOptions = {
          limit: 100,
          offset: 0,
          debug: true,
          type: 'data',
          input: '/tmp/data.json',
          output: baseUrl + '/file_destination_index'
        }

        var mappingDumper = new Elasticdump(mappingOptions.input, mappingOptions.output, mappingOptions)
        var dataDumper = new Elasticdump(dataOptions.input, dataOptions.output, dataOptions)

        mappingDumper.on('error', function (error) { throw (error) })
        dataDumper.on('error', function (error) { throw (error) })

        jobs.push(function (next) { mappingDumper.dump(next) })
        jobs.push(function (next) { setTimeout(next, 5001) })
        jobs.push(function (next) { dataDumper.dump(next) })
        jobs.push(function (next) { setTimeout(next, 5001) })

        async.series(jobs, done)
      })

      it('the dump transfered', function (done) {
        var url = baseUrl + '/file_destination_index/_search'
        request.get(url, function (err, response, body) {
          should.not.exist(err)
          body = JSON.parse(body)
          // this confirms that there are no orphans too!
          body.hits.total.should.equal(cities.length + (cities.length * people.length))
          done()
        })
      })

      describe('each city should have children', function () {
        cities.forEach(function (city) {
          it(city + ' should have children', function (done) {
            var url = baseUrl + '/file_destination_index/_search'
            var payload = {
              'query': {
                'has_parent': {
                  'parent_type': 'city',
                  'query': {
                    'wildcard': {
                      'name': ''
                    }
                  }
                }
              }
            }
            payload.query.has_parent.query.wildcard.name = '*' + city + '*'

            request.get(url, {body: JSON.stringify(payload)}, function (err, response, body) {
              should.not.exist(err)
              body = JSON.parse(body)
              body.hits.total.should.equal(people.length)
              done()
            })
          })
        })
      })
    })
  })
})
