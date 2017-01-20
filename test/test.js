var http = require('http')
http.globalAgent.maxSockets = 10

var path = require('path')
var Elasticdump = require(path.join(__dirname, '..', 'elasticdump.js'))
var request = require('request')
var should = require('should')
var fs = require('fs')
var os = require('os')
var async = require('async')
var baseUrl = 'http://127.0.0.1:9200'

var seeds = {}
var seedSize = 500
var testTimeout = seedSize * 100
var i = 0
var indexesExistingBeforeSuite = 0

while (i < seedSize) {
  seeds[i] = { key: ('key' + i) }
  i++
}

var seed = function (index, type, settings, callback) {
  var payload = {url: baseUrl + '/' + index, body: JSON.stringify(settings)}
  request.put(payload, function (err, response) { // create the index first with potential custom analyzers before seeding
    should.not.exist(err)
    var started = 0
    for (var key in seeds) {
      started++
      var s = seeds[key]
      s['_uuid'] = key
      var url = baseUrl + '/' + index + '/' + type + '/' + key
      request.put(url, {body: JSON.stringify(s)}, function (err, response, body) {
        should.not.exist(err)
        started--
        if (started === 0) {
          request.post(baseUrl + '/' + index + '/_refresh', function (err, response) {
            should.not.exist(err)
            callback()
          })
        }
      })
    }
  })
}

var clear = function (callback) {
  request.del(baseUrl + '/destination_index', function (err, response, body) {
    should.not.exist(err)
    request.del(baseUrl + '/source_index', function (err, response, body) {
      should.not.exist(err)
      request.del(baseUrl + '/another_index', function (err, response, body) {
        should.not.exist(err)
        callback()
      })
    })
  })
}

describe('ELASTICDUMP', function () {
  before(function (done) {
    request(baseUrl + '/_cat/indices', function (err, response, body) {
      should.not.exist(err)
      var lines = body.split('\n')
      lines.forEach(function (line) {
        var words = line.split(' ')
        var index = words[2]
        if (line.length > 0 && ['source_index', 'another_index', 'destination_index'].indexOf(index) < 0) {
          indexesExistingBeforeSuite++
        }
      })
      done()
    })
  })

  beforeEach(function (done) {
    this.timeout(testTimeout)
    clear(function () {
      var settings = {
        'settings': {
          'analysis': {
            'analyzer': {
              'content': {
                'type': 'custom',
                'tokenizer': 'whitespace'
              }
            }
          }
        }
      } // settings for index to be created with
      seed('source_index', 'seeds', settings, function () {
        seed('another_index', 'seeds', undefined, function () {
          setTimeout(function () {
            done()
          }, 500)
        })
      })
    })
  })

  it('can connect', function (done) {
    this.timeout(testTimeout)
    request(baseUrl, function (err, response, body) {
      should.not.exist(err)
      body = JSON.parse(body)
      body.tagline.should.equal('You Know, for Search')
      done()
    })
  })

  it('source_index starts filled', function (done) {
    this.timeout(testTimeout)
    var url = baseUrl + '/source_index/_search'
    request.get(url, function (err, response, body) {
      should.not.exist(err)
      body = JSON.parse(body)
      body.hits.total.should.equal(seedSize)
      done()
    })
  })

  it('destination_index starts non-existant', function (done) {
    this.timeout(testTimeout)
    var url = baseUrl + '/destination_index/_search'
    request.get(url, function (err, response, body) {
      should.not.exist(err)
      body = JSON.parse(body)
      body.status.should.equal(404)
      done()
    })
  })

  it('sets User-Agent', function (done) {
    var Elasticsearch = require(path.join(__dirname, '../lib/transports', 'elasticsearch'))['elasticsearch']
    this.timeout(testTimeout)
    var parent = { options: { searchBody: 'none' } }
    var es = (new Elasticsearch(parent, baseUrl, 'source_index'))
    es.baseRequest(baseUrl, function (err, response, body) {
      should.not.exist(err)
      response.req._headers['user-agent'].should.equal('elasticdump')
      done()
    })
  })

  describe('es to es', function () {
    it('works for a whole index', function (done) {
      this.timeout(testTimeout)
      var options = {
        limit: 100,
        offset: 0,
        debug: false,
        type: 'data',
        input: baseUrl + '/source_index',
        output: baseUrl + '/destination_index',
        scrollTime: '10m'
      }

      var dumper = new Elasticdump(options.input, options.output, options)

      dumper.dump(function () {
        var url = baseUrl + '/destination_index/_search'
        request.get(url, function (err, response, body) {
          should.not.exist(err)
          body = JSON.parse(body)
          body.hits.total.should.equal(seedSize)
          done()
        })
      })
    })

    it('can provide offset', function (done) {
      this.timeout(testTimeout)
      var options = {
        limit: 100,
        debug: false,
        type: 'data',
        input: baseUrl + '/source_index',
        output: baseUrl + '/destination_index',
        scrollTime: '10m',
        offset: 250
      }

      var dumper = new Elasticdump(options.input, options.output, options)

      dumper.dump(function () {
        var url = baseUrl + '/destination_index/_search'
        request.get(url, function (err, response, body) {
          should.not.exist(err)
          body = JSON.parse(body)
          body.hits.total.should.equal(seedSize - 250)
          done()
        })
      })
    })

    it('works for index/types', function (done) {
      this.timeout(testTimeout)
      var options = {
        limit: 100,
        offset: 0,
        debug: false,
        type: 'data',
        input: baseUrl + '/source_index/seeds',
        output: baseUrl + '/destination_index',
        scrollTime: '10m'
      }

      var dumper = new Elasticdump(options.input, options.output, options)

      dumper.dump(function () {
        var url = baseUrl + '/destination_index/_search'
        request.get(url, function (err, response, body) {
          should.not.exist(err)
          body = JSON.parse(body)
          body.hits.total.should.equal(seedSize)
          done()
        })
      })
    })

    it('works for index/types in separate option', function (done) {
      this.timeout(testTimeout)
      var options = {
        limit: 100,
        offset: 0,
        debug: false,
        type: 'data',
        input: baseUrl,
        'input-index': '/source_index/seeds',
        output: baseUrl,
        'output-index': '/destination_index',
        scrollTime: '10m'
      }

      var dumper = new Elasticdump(options.input, options.output, options)

      dumper.dump(function () {
        var url = baseUrl + '/destination_index/_search'
        request.get(url, function (err, response, body) {
          should.not.exist(err)
          body = JSON.parse(body)
          body.hits.total.should.equal(seedSize)
          done()
        })
      })
    })

    it('works with searchBody', function (done) {
      this.timeout(testTimeout)
      var options = {
        limit: 100,
        offset: 0,
        debug: false,
        type: 'data',
        input: baseUrl + '/source_index/seeds',
        output: baseUrl + '/destination_index',
        scrollTime: '10m',
        searchBody: { 'query': { 'term': { 'key': 'key1' } } }
      }

      var dumper = new Elasticdump(options.input, options.output, options)

      dumper.dump(function () {
        var url = baseUrl + '/destination_index/_search'
        request.get(url, function (err, response, body) {
          should.not.exist(err)
          body = JSON.parse(body)
          body.hits.total.should.equal(1)
          done()
        })
      })
    })

    it('works with searchBody range', function (done) {
      // Test Note: Since UUID is ordered as string, lte: 2 should return _uuids 0,1,2,    10-19,  100-199 for a total of 113
      this.timeout(testTimeout)
      var options = {
        limit: 100,
        offset: 0,
        debug: false,
        type: 'data',
        input: baseUrl + '/source_index/seeds',
        output: baseUrl + '/destination_index',
        scrollTime: '10m',
        searchBody: { 'query': { 'range': { '_uuid': { 'lte': '2' } } } }
      }

      var dumper = new Elasticdump(options.input, options.output, options)

      dumper.dump(function () {
        var url = baseUrl + '/destination_index/_search'
        request.get(url, function (err, response, body) {
          should.not.exist(err)
          body = JSON.parse(body)
          body.hits.total.should.equal(113)
          done()
        })
      })
    })

    it('can get and set analyzer', function (done) {
      this.timeout(testTimeout)
      var options = {
        limit: 100,
        offset: 0,
        debug: false,
        type: 'analyzer',
        input: baseUrl + '/source_index',
        output: baseUrl + '/destination_index',
        scrollTime: '10m'
      }
      var dumper = new Elasticdump(options.input, options.output, options)

      dumper.dump(function () {
        // Use async's whilst module to ensure that index is for sure opened after setting analyzers
        // opening an index has a delay
        var status = false
        async.whilst(
          function () { return !status },
          function (callback) {
            var url = baseUrl + '/destination_index/_search'
            request.get(url, function (err, response, body) {
              should.not.exist(err)
              body = JSON.parse(body)
              try {
                body.hits.total.should.equal(0)
                status = true
              } catch (err) {
                status = false
              }
              callback(null, status)
            })
          },
          function (err, n) {
            should.not.exist(err)
            var url = baseUrl + '/destination_index/_settings'
            request.get(url, function (err, response, body) {
              should.not.exist(err)
              body = JSON.parse(body)
              body.destination_index.settings.index.analysis.analyzer.content.type.should.equal('custom')
              done()
            })
          }
        )
      })
    })

    it('can get and set mapping', function (done) {
      this.timeout(testTimeout)
      var options = {
        limit: 100,
        offset: 0,
        debug: false,
        type: 'mapping',
        input: baseUrl + '/source_index',
        output: baseUrl + '/destination_index',
        scrollTime: '10m'
      }

      var dumper = new Elasticdump(options.input, options.output, options)

      dumper.dump(function () {
        var url = baseUrl + '/destination_index/_search'
        request.get(url, function (err, response, body) {
          should.not.exist(err)
          body = JSON.parse(body)
          body.hits.total.should.equal(0)
          var url = baseUrl + '/destination_index/_mapping'
          request.get(url, function (err, response, body) {
            should.not.exist(err)
            body = JSON.parse(body);
            ['string', 'text'].should.containEql(body.destination_index.mappings.seeds.properties.key.type)
            done()
          })
        })
      })
    })

    it('works with a small limit', function (done) {
      this.timeout(testTimeout)
      var options = {
        limit: 10,
        offset: 0,
        debug: false,
        type: 'data',
        input: baseUrl + '/source_index',
        output: baseUrl + '/destination_index',
        scrollTime: '10m'
      }

      var dumper = new Elasticdump(options.input, options.output, options)

      dumper.dump(function () {
        var url = baseUrl + '/destination_index/_search'
        request.get(url, function (err, response, body) {
          should.not.exist(err)
          body = JSON.parse(body)
          body.hits.total.should.equal(seedSize)
          done()
        })
      })
    })

    it('works with a large limit', function (done) {
      this.timeout(testTimeout)
      var options = {
        limit: (10000 - 1),
        offset: 0,
        debug: false,
        type: 'data',
        input: baseUrl + '/source_index',
        output: baseUrl + '/destination_index',
        scrollTime: '10m'
      }

      var dumper = new Elasticdump(options.input, options.output, options)

      dumper.dump(function () {
        var url = baseUrl + '/destination_index/_search'
        request.get(url, function (err, response, body) {
          should.not.exist(err)
          body = JSON.parse(body)
          body.hits.total.should.equal(seedSize)
          done()
        })
      })
    })

    it('counts updates as writes', function (done) {
      this.timeout(testTimeout)
      var options = {
        limit: 100,
        offset: 0,
        debug: false,
        type: 'data',
        input: baseUrl + '/source_index',
        output: baseUrl + '/destination_index',
        scrollTime: '10m'
      }

      var dumperA = new Elasticdump(options.input, options.output, options)
      var dumperB = new Elasticdump(options.input, options.output, options)

      dumperA.dump(function (err, totalWrites) {
        should.not.exist(err)
        var url = baseUrl + '/destination_index/_search'
        request.get(url, function (err, response, body) {
          should.not.exist(err)
          body = JSON.parse(body)
          body.hits.total.should.equal(seedSize)
          totalWrites.should.equal(seedSize)

          dumperB.dump(function (err, totalWrites) {
            should.not.exist(err)
            var url = baseUrl + '/destination_index/_search'
            request.get(url, function (err, response, body) {
              should.not.exist(err)
              body = JSON.parse(body)
              body.hits.total.should.equal(seedSize)
              totalWrites.should.equal(seedSize)
              done()
            })
          })
        })
      })
    })

    it('can also delete documents from the source index', function (done) {
      this.timeout(testTimeout)
      var options = {
        limit: 100,
        offset: 0,
        debug: false,
        type: 'data',
        delete: true,
        input: baseUrl + '/source_index',
        output: baseUrl + '/destination_index',
        scrollTime: '10m'
      }

      var dumper = new Elasticdump(options.input, options.output, options)

      dumper.dump(function () {
        var url = baseUrl + '/destination_index/_search'
        request.get(url, function (err, response, destinationBody) {
          should.not.exist(err)
          destinationBody = JSON.parse(destinationBody)
          destinationBody.hits.total.should.equal(seedSize)
          dumper.input.reindex(function () {
            // Note: Depending on the speed of your ES server
            // all the elements might not be deleted when the HTTP response returns
            // sleeping is required, but the duration is based on your CPU, disk, etc.
            // lets guess 1ms per entry in the index
            setTimeout(function () {
              var url = baseUrl + '/source_index/_search'
              request.get(url, function (err, response, sourceBody) {
                should.not.exist(err)
                sourceBody = JSON.parse(sourceBody)
                sourceBody.hits.total.should.equal(0)
                done()
              })
            }, 5 * seedSize)
          })
        })
      })
    })
  })

  describe('es to file', function () {
    it('works', function (done) {
      this.timeout(testTimeout)
      var options = {
        limit: 100,
        offset: 0,
        debug: false,
        type: 'data',
        input: baseUrl + '/source_index',
        output: '/tmp/out.json',
        scrollTime: '10m',
        sourceOnly: false,
        jsonLines: false
      }

      if (fs.existsSync('/tmp/out.json')) { fs.unlinkSync('/tmp/out.json') }

      var dumper = new Elasticdump(options.input, options.output, options)

      dumper.dump(function () {
        var raw = fs.readFileSync('/tmp/out.json')
        var lineCount = String(raw).split('\n').length
        lineCount.should.equal(seedSize + 1)
        done()
      })
    })
  })

  describe('es to file sourceOnly', function () {
    it('works', function (done) {
      this.timeout(testTimeout)
      var options = {
        limit: 100,
        offset: 0,
        debug: false,
        type: 'data',
        input: baseUrl + '/source_index',
        output: '/tmp/out.sourceOnly',
        scrollTime: '10m',
        sourceOnly: true,
        jsonLines: false
      }

      if (fs.existsSync('/tmp/out.sourceOnly')) { fs.unlinkSync('/tmp/out.sourceOnly') }

      var dumper = new Elasticdump(options.input, options.output, options)

      dumper.dump(function () {
        var raw = fs.readFileSync('/tmp/out.sourceOnly')
        var lines = String(raw).split('\n')
        lines.length.should.equal(seedSize + 1)

        // "key" should be immediately available
        var first = JSON.parse(lines[0])
        first['key'].length.should.be.above(0)
        done()
      })
    })
  })

  describe('file to es', function () {
    it('works', function (done) {
      this.timeout(testTimeout)
      var options = {
        limit: 100,
        offset: 0,
        debug: false,
        type: 'data',
        input: '/tmp/out.json',
        output: baseUrl + '/destination_index',
        scrollTime: '10m'
      }

      var dumper = new Elasticdump(options.input, options.output, options)

      dumper.dump(function () {
        var url = baseUrl + '/destination_index/_search'
        request.get(url, function (err, response, body) {
          should.not.exist(err)
          body = JSON.parse(body)
          body.hits.total.should.equal(seedSize)
          done()
        })
      })
    })

    it('can provide offset', function (done) {
      this.timeout(testTimeout)
      var options = {
        limit: 100,
        debug: false,
        type: 'data',
        input: '/tmp/out.json',
        output: baseUrl + '/destination_index',
        scrollTime: '10m',
        offset: 250
      }

      var dumper = new Elasticdump(options.input, options.output, options)

      dumper.dump(function (error) {
        should.not.exist(error)
        var url = baseUrl + '/destination_index/_search'
        request.get(url, function (err, response, body) {
          should.not.exist(err)
          body = JSON.parse(body)
          // skips 250 so 250 less in there
          body.hits.total.should.equal(seedSize - 250)
          done()
        })
      })
    })
  })

  describe('all es to file', function () {
    it('works', function (done) {
      if (indexesExistingBeforeSuite > 0) {
        console.log('')
        console.log('! ' + indexesExistingBeforeSuite + ' ES indeses detected')
        console.log('! Skipping this test as your ES cluster has more indexes than just the test indexes')
        console.log('! Please empty your local elasticsearch and retry this test')
        console.log('')
        done()
      } else {
        this.timeout(testTimeout)
        var options = {
          limit: 100,
          offset: 0,
          debug: false,
          type: 'data',
          input: baseUrl,
          output: '/tmp/out.json',
          scrollTime: '10m',
          sourceOnly: false,
          jsonLines: false,
          all: true
        }

        if (fs.existsSync('/tmp/out.json')) { fs.unlinkSync('/tmp/out.json') }

        var dumper = new Elasticdump(options.input, options.output, options)

        dumper.dump(function () {
          var raw = fs.readFileSync('/tmp/out.json')
          var output = []
          raw.toString().split(os.EOL).forEach(function (line) {
            if (line.length > 0) {
              output.push(JSON.parse(line))
            }
          })

          var count = 0
          for (var i in output) {
            var elem = output[i]
            if (elem['_index'] === 'source_index' || elem['_index'] === 'another_index') {
              count++
            }
          }

          count.should.equal(seedSize * 2)
          done()
        })
      }
    })
  })

  describe('es to stdout', function () {
    it('works')
  })

  describe('stdin to es', function () {
    it('works')
  })
})
