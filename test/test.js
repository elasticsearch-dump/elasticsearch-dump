const http = require('http')
http.globalAgent.maxSockets = 10

const path = require('path')
const Elasticdump = require(path.join(__dirname, '..', 'elasticdump.js'))
const jsonParser = require('../lib/jsonparser.js')
const should = require('should')
const fs = require('fs')
const os = require('os')
const async = require('async')
const _ = require('lodash')
const jq = require('jsonpath')
const baseUrl = 'http://127.0.0.1:9200'

const seeds = {}
const seedSize = 500
const testTimeout = seedSize * 200
let i = 0
let indexesExistingBeforeSuite = 0

while (i < seedSize) {
  seeds[i] = { key: ('key' + i) }
  i++
}

const request = require('request').defaults({
  headers: {
    'User-Agent': 'elasticdump',
    'Content-Type': 'application/json'
  }
})

const seed = (index, type, settings, callback) => {
  const payload = { url: baseUrl + '/' + index, body: JSON.stringify(settings) }
  request.put(payload, (err, response) => { // create the index first with potential custom analyzers before seeding
    should.not.exist(err)
    let started = 0
    for (const key in seeds) {
      started++
      const s = seeds[key]
      s._uuid = key
      const url = baseUrl + '/' + index + '/' + type + '/' + key
      request.put(url, { body: JSON.stringify(s) }, (err, response, body) => {
        should.not.exist(err)
        started--
        if (started === 0) {
          request.post(baseUrl + '/' + index + '/_refresh', (err, response) => {
            should.not.exist(err)
            callback()
          })
        }
      })
    }
  })
}

const loadTemplate = (templateName, templateBody, callback) => {
  const payload = { url: baseUrl + '/_template/' + templateName, body: JSON.stringify(templateBody) }
  request.put(payload, (err, response) => { // create the index first with potential custom analyzers before seeding
    should.not.exist(err)
    callback()
  })
}

const clear = callback => {
  request.del(baseUrl + '/destination_index', (err, response, body) => {
    should.not.exist(err)
    request.del(baseUrl + '/source_index', (err, response, body) => {
      should.not.exist(err)
      request.del(baseUrl + '/another_index', (err, response, body) => {
        should.not.exist(err)
        callback()
      })
    })
  })
}

const getTotal = (body) => _.get(body, 'hits.total.value', body.hits.total)

describe('ELASTICDUMP', () => {
  before(done => {
    request(baseUrl + '/_cat/indices', (err, response, body) => {
      should.not.exist(err)
      const lines = body.split('\n')
      lines.forEach(line => {
        const words = line.split(' ')
        const index = words[2]
        if (line.length > 0 && ['source_index', 'another_index', 'destination_index'].indexOf(index) < 0) {
          indexesExistingBeforeSuite++
        }
      })
      done()
    })
  })

  beforeEach(function (done) {
    this.timeout(testTimeout)
    clear(() => {
      const settings = {
        settings: {
          analysis: {
            analyzer: {
              content: {
                type: 'custom',
                tokenizer: 'whitespace'
              }
            }
          }
        }
      }

      const isNew = /[6-9]\.\d+\..+/.test(process.env.ES_VERSION)

      const templateSettings = {
        [isNew ? 'index_patterns' : 'template']: isNew ? [
          'source_index',
          'another_index',
          'destination_index'
        ] : '*_index',
        settings: {
          number_of_shards: 1,
          number_of_replicas: 0
        }
      }

      // settings for index to be created with
      loadTemplate('template_1', templateSettings, () => {
        seed('source_index', 'seeds', settings, () => {
          seed('another_index', 'seeds', undefined, () => {
            setTimeout(() => {
              done()
            }, 500)
          })
        })
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

  it('source_index starts filled', function (done) {
    this.timeout(testTimeout)
    const url = baseUrl + '/source_index/_search'
    request.get(url, (err, response, body) => {
      should.not.exist(err)
      body = JSON.parse(body)
      getTotal(body).should.equal(seedSize)
      done()
    })
  })

  it('destination_index starts non-existant', function (done) {
    this.timeout(testTimeout)
    const url = baseUrl + '/destination_index/_search'
    request.get(url, (err, response, body) => {
      should.not.exist(err)
      body = JSON.parse(body)
      body.status.should.equal(404)
      done()
    })
  })

  it('sets User-Agent', function (done) {
    const Elasticsearch = require(path.join(__dirname, '../lib/transports', 'elasticsearch')).elasticsearch
    this.timeout(testTimeout)
    const parent = { options: { searchBody: 'none' } }
    const es = (new Elasticsearch(parent, baseUrl, 'source_index'))
    es.baseRequest(baseUrl, (err, response, body) => {
      should.not.exist(err)
      response.req._headers['user-agent'].should.equal('elasticdump')
      done()
    })
  })

  it('sets custom headers', function (done) {
    const Elasticsearch = require(path.join(__dirname, '../lib/transports', 'elasticsearch')).elasticsearch
    this.timeout(testTimeout)
    const parent = { options: { searchBody: 'none' } }
    const opts = {
      index: 'source_index',
      headers: {
        'User-Agent': 'testbot',
        'Alt-Auth': 'SomeBearerToken',
        'X-Something': 'anotherheader'
      }
    }
    const es = (new Elasticsearch(parent, baseUrl, opts))
    es.baseRequest(baseUrl, (err, response, body) => {
      should.not.exist(err)
      response.req._headers['user-agent'].should.equal('testbot')
      response.req._headers['alt-auth'].should.equal('SomeBearerToken')
      response.req._headers['x-something'].should.equal('anotherheader')
      done()
    })
  })

  it('sets custom params', function (done) {
    const Elasticsearch = require(path.join(__dirname, '../lib/transports', 'elasticsearch')).elasticsearch
    this.timeout(testTimeout)
    const parent = {
      options: {
        searchBody: {},
        scrollTime: '1m',
        params: {
          preference: '_shards:0'
        }
      },
      emit: (level, msg) => {
        console[level](msg)
      }
    }
    const opts = {
      index: 'source_index',
      headers: {
        'Content-Type': 'application/json'
      }
    }
    const es = (new Elasticsearch(parent, baseUrl, opts))
    es.getData(1, 0, (err, responseBody, response) => {
      should.not.exist(err)
      response.req.path.should.containEql('preference=_shards:0')
      done()
    })
  })

  describe('es to es', () => {
    it('works for a whole index', function (done) {
      this.timeout(testTimeout)
      const options = {
        limit: 100,
        offset: 0,
        debug: false,
        type: 'data',
        input: baseUrl + '/source_index',
        output: baseUrl + '/destination_index',
        scrollTime: '10m'
      }

      const dumper = new Elasticdump(options.input, options.output, options)

      dumper.dump(() => {
        const url = baseUrl + '/destination_index/_search'
        request.get(url, (err, response, body) => {
          should.not.exist(err)
          body = JSON.parse(body)
          getTotal(body).should.equal(seedSize)
          done()
        })
      })
    })

    it('can provide offset', function (done) {
      if (/[6-9]\.\d+\..+/.test(process.env.ES_VERSION)) {
        return this.skip()
      }
      this.timeout(testTimeout)
      const options = {
        limit: 100,
        debug: false,
        type: 'data',
        input: baseUrl + '/source_index',
        output: baseUrl + '/destination_index',
        scrollTime: '10m',
        offset: 250
      }

      const dumper = new Elasticdump(options.input, options.output, options)

      dumper.dump(() => {
        const url = baseUrl + '/destination_index/_search'
        request.get(url, (err, response, body) => {
          should.not.exist(err)
          body = JSON.parse(body)
          getTotal(body).should.equal(seedSize - 250)
          done()
        })
      })
    })

    it('can provide limit', function (done) {
      this.timeout(testTimeout)
      const options = {
        limit: 100,
        offset: 0,
        debug: false,
        type: 'data',
        input: baseUrl + '/source_index/seeds',
        output: baseUrl + '/destination_index',
        size: 5,
        scrollTime: '10m'
      }

      const dumper = new Elasticdump(options.input, options.output, options)

      dumper.dump(() => {
        const url = baseUrl + '/destination_index/_search'
        request.get(url, (err, response, body) => {
          should.not.exist(err)
          body = JSON.parse(body)
          getTotal(body).should.equal(5)
          done()
        })
      })
    })

    it('works for index/types', function (done) {
      this.timeout(testTimeout)
      const options = {
        limit: 100,
        offset: 0,
        debug: false,
        type: 'data',
        input: baseUrl + '/source_index/seeds',
        output: baseUrl + '/destination_index',
        scrollTime: '10m'
      }

      const dumper = new Elasticdump(options.input, options.output, options)

      dumper.dump(() => {
        const url = baseUrl + '/destination_index/_search'
        request.get(url, (err, response, body) => {
          should.not.exist(err)
          body = JSON.parse(body)
          getTotal(body).should.equal(seedSize)
          done()
        })
      })
    })

    it('works for index/types in separate option', function (done) {
      this.timeout(testTimeout)
      const options = {
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

      const dumper = new Elasticdump(options.input, options.output, options)

      dumper.dump(() => {
        const url = baseUrl + '/destination_index/_search'
        request.get(url, (err, response, body) => {
          should.not.exist(err)
          body = JSON.parse(body)
          getTotal(body).should.equal(seedSize)
          done()
        })
      })
    })

    it('works with searchBody', function (done) {
      this.timeout(testTimeout)
      const options = {
        limit: 100,
        offset: 0,
        debug: false,
        type: 'data',
        input: baseUrl + '/source_index/seeds',
        output: baseUrl + '/destination_index',
        scrollTime: '10m',
        searchBody: { query: { term: { key: 'key1' } } }
      }

      const dumper = new Elasticdump(options.input, options.output, options)

      dumper.dump(() => {
        const url = baseUrl + '/destination_index/_search'
        request.get(url, (err, response, body) => {
          should.not.exist(err)
          body = JSON.parse(body)
          getTotal(body).should.equal(1)
          done()
        })
      })
    })

    it('works with searchBody range', function (done) {
      // Test Note: Since UUID is ordered as string, lte: 2 should return _uuids 0,1,2,    10-19,  100-199 for a total of 113
      this.timeout(testTimeout)
      const options = {
        limit: 100,
        offset: 0,
        debug: false,
        type: 'data',
        input: baseUrl + '/source_index/seeds',
        output: baseUrl + '/destination_index',
        scrollTime: '10m',
        searchBody: { query: { range: { _uuid: { lte: '2' } } } }
      }

      const dumper = new Elasticdump(options.input, options.output, options)

      dumper.dump(() => {
        const url = baseUrl + '/destination_index/_search'
        request.get(url, (err, response, body) => {
          should.not.exist(err)
          body = JSON.parse(body)
          getTotal(body).should.equal(113)
          done()
        })
      })
    })

    it('can get and set analyzer', function (done) {
      this.timeout(testTimeout)
      const options = {
        limit: 100,
        offset: 0,
        debug: false,
        type: 'analyzer',
        input: baseUrl + '/source_index',
        output: baseUrl + '/destination_index',
        scrollTime: '10m'
      }
      const dumper = new Elasticdump(options.input, options.output, options)

      dumper.dump(() => {
        // Use async's whilst module to ensure that index is for sure opened after setting analyzers
        // opening an index has a delay
        let status = false
        async.whilst(
          () => !status,
          callback => {
            const url = baseUrl + '/destination_index/_search'
            request.get(url, (err, response, body) => {
              should.not.exist(err)
              body = JSON.parse(body)
              try {
                getTotal(body).should.equal(0)
                status = true
              } catch (err) {
                status = false
              }
              callback(null, status)
            })
          },
          (err, n) => {
            should.not.exist(err)
            const url = baseUrl + '/destination_index/_settings'
            request.get(url, (err, response, body) => {
              should.not.exist(err)
              body = JSON.parse(body)
              body.destination_index.settings.index.analysis.analyzer.content.type.should.equal('custom')
              done()
            })
          }
        )
      })
    })

    it('can get and set settings', function (done) {
      this.timeout(testTimeout)
      const options = {
        limit: 100,
        offset: 0,
        debug: false,
        type: 'settings',
        input: baseUrl + '/source_index',
        output: baseUrl + '/destination_index',
        scrollTime: '10m'
      }
      const dumper = new Elasticdump(options.input, options.output, options)

      dumper.dump(() => {
        // Use async's whilst module to ensure that index is for sure opened after setting analyzers
        // opening an index has a delay
        let status = false
        async.whilst(
          () => !status,
          callback => {
            const url = baseUrl + '/destination_index/_search'
            request.get(url, (err, response, body) => {
              should.not.exist(err)
              body = JSON.parse(body)
              try {
                getTotal(body).should.equal(0)
                status = true
              } catch (err) {
                status = false
              }
              callback(null, status)
            })
          },
          (err, n) => {
            should.not.exist(err)
            const url = baseUrl + '/destination_index/_settings'
            request.get(url, (err, response, body) => {
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
      const options = {
        limit: 100,
        offset: 0,
        debug: false,
        type: 'mapping',
        input: baseUrl + '/source_index',
        output: baseUrl + '/destination_index',
        scrollTime: '10m'
      }

      const dumper = new Elasticdump(options.input, options.output, options)

      dumper.dump(() => {
        const url = baseUrl + '/destination_index/_search'
        request.get(url, (err, response, body) => {
          should.not.exist(err)
          body = JSON.parse(body)
          getTotal(body).should.equal(0)
          const url = baseUrl + '/destination_index/_mapping'
          request.get(url, (err, response, body) => {
            should.not.exist(err)
            body = JSON.parse(body);
            ['string', 'text'].should.containEql(jq.value(body, 'destination_index.mappings..properties.key.type'))
            done()
          })
        })
      })
    })

    it('can set and get alias', function (done) {
      this.timeout(testTimeout)
      const aliasFilePath = path.join(__dirname, 'test-resources', 'alias.json')
      const options = {
        limit: 100,
        offset: 0,
        debug: false,
        type: 'alias',
        input: aliasFilePath,
        output: baseUrl
      }

      const dumper = new Elasticdump(options.input, options.output, options)

      dumper.dump(() => {
        const url = baseUrl + '/source_index/_alias/*'
        request.get(url, (err, response, body) => {
          should.not.exist(err)
          const raw = fs.readFileSync(aliasFilePath)
          body = JSON.parse(body)
          body.should.deepEqual(JSON.parse(JSON.parse(raw.toString())))
          done()
        })
      })
    })

    it('can set and get template', function (done) {
      this.timeout(testTimeout)

      let templateFile = 'template_2x.json'
      if (process.env.ES_VERSION === '1.5.0') {
        templateFile = 'template_1x.json'
      } else if (process.env.ES_VERSION === '6.0.0') {
        templateFile = 'template_6x.json'
      } else if (/[7-9]\.\d+\..+/.test(process.env.ES_VERSION)) {
        templateFile = 'template_7x.json'
      }

      const templateFilePath = path.join(__dirname, 'test-resources', templateFile)
      const options = {
        limit: 100,
        offset: 0,
        debug: false,
        type: 'template',
        input: templateFilePath,
        output: baseUrl
      }

      const dumper = new Elasticdump(options.input, options.output, options)

      dumper.dump(() => {
        const url = baseUrl + '/_template/template_1'
        request.get(url, (err, response, body) => {
          should.not.exist(err)
          const raw = fs.readFileSync(templateFilePath)
          body = JSON.parse(body)
          body.should.deepEqual(JSON.parse(JSON.parse(raw.toString())))
          done()
        })
      })
    })

    it('works with a small limit', function (done) {
      this.timeout(testTimeout)
      const options = {
        limit: 10,
        offset: 0,
        debug: false,
        type: 'data',
        input: baseUrl + '/source_index',
        output: baseUrl + '/destination_index',
        scrollTime: '10m'
      }

      const dumper = new Elasticdump(options.input, options.output, options)

      dumper.dump(() => {
        const url = baseUrl + '/destination_index/_search'
        request.get(url, (err, response, body) => {
          should.not.exist(err)
          body = JSON.parse(body)
          getTotal(body).should.equal(seedSize)
          done()
        })
      })
    })

    it('works with a large limit', function (done) {
      this.timeout(testTimeout)
      const options = {
        limit: (10000 - 1),
        offset: 0,
        debug: false,
        type: 'data',
        input: baseUrl + '/source_index',
        output: baseUrl + '/destination_index',
        scrollTime: '10m'
      }

      const dumper = new Elasticdump(options.input, options.output, options)

      dumper.dump(() => {
        const url = baseUrl + '/destination_index/_search'
        request.get(url, (err, response, body) => {
          should.not.exist(err)
          body = JSON.parse(body)
          getTotal(body).should.equal(seedSize)
          done()
        })
      })
    })

    it('counts updates as writes', function (done) {
      this.timeout(testTimeout)
      const options = {
        limit: 100,
        offset: 0,
        debug: false,
        type: 'data',
        input: baseUrl + '/source_index',
        output: baseUrl + '/destination_index',
        scrollTime: '10m'
      }

      const dumperA = new Elasticdump(options.input, options.output, options)
      const dumperB = new Elasticdump(options.input, options.output, options)

      dumperA.dump((err, totalWrites) => {
        should.not.exist(err)
        const url = baseUrl + '/destination_index/_search'
        request.get(url, (err, response, body) => {
          should.not.exist(err)
          body = JSON.parse(body)
          getTotal(body).should.equal(seedSize)
          totalWrites.should.equal(seedSize)

          dumperB.dump((err, totalWrites) => {
            should.not.exist(err)
            const url = baseUrl + '/destination_index/_search'
            request.get(url, (err, response, body) => {
              should.not.exist(err)
              body = JSON.parse(body)
              getTotal(body).should.equal(seedSize)
              totalWrites.should.equal(seedSize)
              done()
            })
          })
        })
      })
    })

    it('noRefresh option', function (done) {
      this.timeout(testTimeout)
      const options = {
        limit: 100,
        offset: 0,
        debug: true,
        type: 'data',
        input: baseUrl + '/source_index',
        output: baseUrl + '/destination_index',
        scrollTime: '10m',
        noRefresh: true
      }

      const dumper = new Elasticdump(options.input, options.output, options)
      const inputProto = Object.getPrototypeOf(dumper.input)
      const originalReindex = inputProto.reindex
      inputProto.reindex = function (callback) {
        inputProto.reindex = originalReindex
        originalReindex.call(this, (err, response) => {
          if (err) {
            done(err)
          } else if (response) {
            done('refresh occured')
          } else {
            callback()
          }
        })
      }

      dumper.dump(() => {
        done()
      })
    })

    it('can also delete documents from the source index', function (done) {
      this.timeout(testTimeout)
      const options = {
        limit: 100,
        offset: 0,
        debug: false,
        type: 'data',
        delete: true,
        input: baseUrl + '/source_index',
        output: baseUrl + '/destination_index',
        scrollTime: '10m'
      }

      const dumper = new Elasticdump(options.input, options.output, options)

      dumper.dump(() => {
        const url = baseUrl + '/destination_index/_search'
        request.get(url, (err, response, destinationBody) => {
          should.not.exist(err)
          destinationBody = JSON.parse(destinationBody)
          getTotal(destinationBody).should.equal(seedSize)
          dumper.input.reindex(() => {
            // Note: Depending on the speed of your ES server
            // all the elements might not be deleted when the HTTP response returns
            // sleeping is required, but the duration is based on your CPU, disk, etc.
            // lets guess 1ms per entry in the index
            setTimeout(() => {
              const url = baseUrl + '/source_index/_search'
              request.get(url, (err, response, sourceBody) => {
                should.not.exist(err)
                sourceBody = JSON.parse(sourceBody)
                getTotal(sourceBody).should.equal(0)
                done()
              })
            }, 5 * seedSize)
          })
        })
      })
    })
  })

  describe('es to file', () => {
    it('works', function (done) {
      this.timeout(testTimeout)
      const options = {
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

      const dumper = new Elasticdump(options.input, options.output, options)

      dumper.dump(() => {
        const raw = fs.readFileSync('/tmp/out.json')
        const lineCount = String(raw).split('\n').length
        lineCount.should.equal(seedSize + 1)
        done()
      })
    })
  })

  describe('es to file sourceOnly', () => {
    it('works', function (done) {
      this.timeout(testTimeout)
      const options = {
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

      const dumper = new Elasticdump(options.input, options.output, options)

      dumper.dump(() => {
        const raw = fs.readFileSync('/tmp/out.sourceOnly')
        const lines = String(raw).split('\n')
        lines.length.should.equal(seedSize + 1)

        // "key" should be immediately available
        const first = JSON.parse(lines[0])
        first.key.length.should.be.above(0)
        done()
      })
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
        input: '/tmp/out.json',
        output: baseUrl + '/destination_index',
        scrollTime: '10m'
      }

      const dumper = new Elasticdump(options.input, options.output, options)

      dumper.dump(() => {
        const url = baseUrl + '/destination_index/_search'
        request.get(url, (err, response, body) => {
          should.not.exist(err)
          body = JSON.parse(body)
          getTotal(body).should.equal(seedSize)
          done()
        })
      })
    })

    it('can provide offset', function (done) {
      if (/[6-9]\.\d+\..+/.test(process.env.ES_VERSION)) {
        return this.skip()
      }
      this.timeout(testTimeout)
      const options = {
        limit: 100,
        debug: false,
        type: 'data',
        input: '/tmp/out.json',
        output: baseUrl + '/destination_index',
        scrollTime: '10m',
        offset: 250
      }

      const dumper = new Elasticdump(options.input, options.output, options)

      dumper.dump(error => {
        should.not.exist(error)
        const url = baseUrl + '/destination_index/_search'
        request.get(url, (err, response, body) => {
          should.not.exist(err)
          body = JSON.parse(body)
          // skips 250 so 250 less in there
          getTotal(body).should.equal(seedSize - 250)
          done()
        })
      })
    })
  })

  describe('big int file to es', () => {
    it('works', function (done) {
      this.timeout(testTimeout)

      let options = {
        limit: 100,
        offset: 0,
        debug: false,
        type: 'mapping',
        input: path.join(__dirname, 'test-resources', 'bigint_mapping.json'),
        output: baseUrl + '/bigint_index',
        scrollTime: '10m'
      }

      let dumper = new Elasticdump(options.input, options.output, options)

      dumper.dump(() => {
        options = {
          limit: 100,
          offset: 0,
          debug: false,
          type: 'data',
          input: path.join(__dirname, 'test-resources', 'bigint.json'),
          output: baseUrl + '/bigint_index',
          scrollTime: '10m',
          'support-big-int': true
        }

        dumper = new Elasticdump(options.input, options.output, options)

        dumper.dump(() => {
          const url = baseUrl + '/bigint_index/_search'
          request.get(url, (err, response, body) => {
            should.not.exist(err)
            body = jsonParser.parse(body, { options })
            body.hits.hits.length.should.equal(4)
            _.chain(body.hits.hits)
              .reduce((result, value) => {
                result.push(value._source.key.toString())
                return result
              }, [])
              .sort()
              .value().should.deepEqual([
                '+99926275868403174267',
                '-99926275868403174266',
                '1726275868403174266',
                '99926275868403174266'])
            done()
          })
        })
      })
    })
  })

  describe('big int 2 file to es', () => {
    it('works', function (done) {
      this.timeout(testTimeout)

      let options = {
        limit: 100,
        offset: 0,
        debug: false,
        type: 'mapping',
        input: path.join(__dirname, 'test-resources', 'bigint_mapping2.json'),
        output: baseUrl + '/bigint2_index',
        scrollTime: '10m'
      }

      let dumper = new Elasticdump(options.input, options.output, options)

      dumper.dump(() => {
        options = {
          limit: 100,
          offset: 0,
          debug: false,
          type: 'data',
          input: path.join(__dirname, 'test-resources', 'bigint2.json'),
          output: baseUrl + '/bigint2_index',
          scrollTime: '10m',
          'support-big-int': true,
          'big-int-fields': 'guid'
        }

        dumper = new Elasticdump(options.input, options.output, options)

        dumper.dump(() => {
          const url = baseUrl + '/bigint2_index/_search'
          request.get(url, (err, response, body) => {
            should.not.exist(err)
            body = jsonParser.parse(body, { options })
            body.hits.hits.length.should.equal(1)
            const rec = body.hits.hits[0]
            rec._source.guid.toString().should.eql('647200872369')
            rec._source.nickname.should.eql('01234567891011121314151617181920')
            done()
          })
        })
      })
    })
  })

  describe('big int 3 file to es', () => {
    it('works', function (done) {
      this.timeout(testTimeout)

      let options = {
        limit: 100,
        offset: 0,
        debug: false,
        type: 'mapping',
        input: path.join(__dirname, 'test-resources', 'bigint_mapping3.json'),
        output: baseUrl + '/bigint3_index',
        scrollTime: '10m'
      }

      let dumper = new Elasticdump(options.input, options.output, options)

      dumper.dump(() => {
        options = {
          limit: 100,
          offset: 0,
          debug: false,
          type: 'data',
          input: path.join(__dirname, 'test-resources', 'bigint3.json'),
          output: baseUrl + '/bigint3_index',
          scrollTime: '10m',
          'support-big-int': true
        }

        dumper = new Elasticdump(options.input, options.output, options)

        dumper.dump(() => {
          const url = baseUrl + '/bigint3_index/_search'
          request.get(url, (err, response, body) => {
            should.not.exist(err)
            body = jsonParser.parse(body, { options })
            body.hits.hits.length.should.equal(1)
            const rec = body.hits.hits[0]
            rec._source.guid.should.eql('+01234567891011121314151617181920')
            rec._source.nickname.should.eql('01234567891011121314151617181920')
            done()
          })
        })
      })
    })
  })

  describe('all es to file', () => {
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
        const options = {
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

        const dumper = new Elasticdump(options.input, options.output, options)

        dumper.dump(() => {
          const raw = fs.readFileSync('/tmp/out.json')
          const output = []
          raw.toString().split(os.EOL).forEach(line => {
            if (line.length > 0) {
              output.push(JSON.parse(line))
            }
          })

          let count = 0
          for (const i in output) {
            const elem = output[i]
            if (elem._index === 'source_index' || elem._index === 'another_index') {
              count++
            }
          }

          count.should.equal(seedSize * 2)
          done()
        })
      }
    })
  })

  describe('es to stdout', () => {
    it('works')
  })

  describe('stdin to es', () => {
    it('works')
  })
})
