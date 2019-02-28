'use strict'

/* Test suite for parent-child relationships
 *
 * Parent-Child relations demand a mapping, otherwise the indexation
 * is rejected with an error.
 */

const path = require('path')
const Elasticdump = require(path.join(__dirname, '..', 'elasticdump.js'))

const should = require('should')
const fs = require('fs')
const async = require('async')
const baseUrl = 'http://127.0.0.1:9200'
const indexes = ['source_index', 'destination_index', 'file_destination_index', 'another_index']
const files = ['/tmp/mapping.json', '/tmp/data.json']
const cities = ['new_york', 'san_francisco', 'london', 'tokyo']
const people = ['evan', 'christina', 'pablo', 'brian', 'aaron']
const mapping = {
  city: {},
  person: {_parent: {type: 'city'}}
}

const request = require('request').defaults({
  headers: {
    'User-Agent': 'elasticdump',
    'Content-Type': 'application/json'
  }
})

const clear = callback => {
  const jobs = []
  indexes.forEach(index => {
    jobs.push(done => {
      request.del(baseUrl + '/' + index, done)
    })
  })

  files.forEach(file => {
    jobs.push(done => {
      try {
        fs.unlinkSync(file)
      } catch (e) { }
      done()
    })
  })

  async.series(jobs, callback)
}

const setup = callback => {
  const jobs = []

  jobs.push(done => {
    const url = baseUrl + '/source_index'
    const payload = {mappings: mapping}
    request.put(url, {body: JSON.stringify(payload)}, done)
  })

  cities.forEach(city => {
    jobs.push(done => {
      const url = baseUrl + '/source_index/city/' + city
      const payload = {name: city}
      request.put(url, {body: JSON.stringify(payload)}, done)
    })

    people.forEach(person => {
      jobs.push(done => {
        const url = baseUrl + '/source_index/person/' + person + '_' + city + '?parent=' + city
        const payload = {name: person, city: city}
        request.put(url, {body: JSON.stringify(payload)}, done)
      })
    })
  })

  jobs.push(done => {
    setTimeout(done, 6000)
  })

  async.series(jobs, callback)
}

let describex = describe

if (/[6-9]\.\d+\..+/.test(process.env.ES_VERSION)) {
  // short-circuit the describex to skip-fast
  // if the ES_VERSION is 6
  describex = describe.skip
}

describex('parent child', () => {
  before(function (done) {
    this.timeout(15 * 1000)
    clear(error => {
      if (error) { return done(error) }
      setup(done)
    })
  })

  after(clear)

  it('did the setup properly and parents + children are loaded', done => {
    const url = baseUrl + '/source_index/_search'
    request.get(url, (err, response, body) => {
      should.not.exist(err)
      body = JSON.parse(body)

      // this confirms that there are no orphans too!
      body.hits.total.should.equal(cities.length + (cities.length * people.length))
      done()
    })
  })

  describex('each city should have children', () => {
    cities.forEach(city => {
      it(city + ' should have children', done => {
        const url = baseUrl + '/source_index/_search'
        const payload = {
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

        request.get(url, {body: JSON.stringify(payload)}, (err, response, body) => {
          should.not.exist(err)
          body = JSON.parse(body)
          body.hits.total.should.equal(people.length)
          done()
        })
      })
    })
  })

  describex('ES to ES dump should maintain parent-child relationships', () => {
    before(function (done) {
      this.timeout(2000 * 10)
      const jobs = []

      const mappingOptions = {
        limit: 100,
        offset: 0,
        debug: false,
        type: 'mapping',
        input: baseUrl + '/source_index',
        output: baseUrl + '/destination_index',
        scrollTime: '10m'
      }

      const dataOptions = {
        limit: 100,
        offset: 0,
        debug: false,
        type: 'data',
        input: baseUrl + '/source_index',
        output: baseUrl + '/destination_index',
        scrollTime: '10m'
      }

      const mappingDumper = new Elasticdump(mappingOptions.input, mappingOptions.output, mappingOptions)
      const dataDumper = new Elasticdump(dataOptions.input, dataOptions.output, dataOptions)

      mappingDumper.on('error', error => { throw (error) })
      dataDumper.on('error', error => { throw (error) })

      jobs.push(next => { mappingDumper.dump(next) })
      jobs.push(next => { setTimeout(next, 5001) })
      jobs.push(next => { dataDumper.dump(next) })
      jobs.push(next => { setTimeout(next, 5001) })

      async.series(jobs, done)
    })

    it('the dump transfered', done => {
      const url = baseUrl + '/destination_index/_search'
      request.get(url, (err, response, body) => {
        should.not.exist(err)
        body = JSON.parse(body)
        // this confirms that there are no orphans too!
        body.hits.total.should.equal(cities.length + (cities.length * people.length))
        done()
      })
    })

    describex('each city should have children', () => {
      cities.forEach(city => {
        it(city + ' should have children', done => {
          const url = baseUrl + '/destination_index/_search'
          const payload = {
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

          request.get(url, {body: JSON.stringify(payload)}, (err, response, body) => {
            should.not.exist(err)
            body = JSON.parse(body)
            body.hits.total.should.equal(people.length)
            done()
          })
        })
      })
    })
  })

  describex('ES to File and back to ES should work', () => {
    before(function (done) {
      this.timeout(2000 * 10)
      const jobs = []

      const mappingOptions = {
        limit: 100,
        offset: 0,
        debug: false,
        type: 'mapping',
        input: baseUrl + '/source_index',
        output: '/tmp/mapping.json',
        scrollTime: '10m'
      }

      const dataOptions = {
        limit: 100,
        offset: 0,
        debug: false,
        type: 'data',
        input: baseUrl + '/source_index',
        output: '/tmp/data.json',
        scrollTime: '10m'
      }

      const mappingDumper = new Elasticdump(mappingOptions.input, mappingOptions.output, mappingOptions)
      const dataDumper = new Elasticdump(dataOptions.input, dataOptions.output, dataOptions)

      mappingDumper.on('error', error => { throw (error) })
      dataDumper.on('error', error => { throw (error) })

      jobs.push(next => { mappingDumper.dump(next) })
      jobs.push(next => { setTimeout(next, 5001) })
      jobs.push(next => { dataDumper.dump(next) })
      jobs.push(next => { setTimeout(next, 5001) })

      async.series(jobs, done)
    })

    it('the dump files should have worked', done => {
      const mapping = String(fs.readFileSync('/tmp/mapping.json'))
      const data = String(fs.readFileSync('/tmp/data.json'))
      const mappingLines = []
      const dataLines = []

      mapping.split('\n').forEach(line => {
        if (line.length > 2) { mappingLines.push(JSON.parse(line)) }
      })
      data.split('\n').forEach(line => {
        if (line.length > 2) { dataLines.push(JSON.parse(line)) }
      })

      mappingLines.length.should.equal(1)
      Object.keys(mappingLines[0].source_index.mappings).length.should.equal(2)
      should.not.exist(mappingLines[0].source_index.mappings.city._parent)
      mappingLines[0].source_index.mappings.person._parent.type.should.equal('city')

      const dumpedPeople = []
      const dumpedCties = []
      dataLines.forEach(d => {
        if (d._type === 'person') {
          let parent
          if (d._parent) { parent = d._parent } // ES 2.x
          if (d.fields && d.fields._parent) { parent = d.fields._parent } // ES 1.x
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

    describex('can restore from a dumpfile', () => {
      before(function (done) {
        this.timeout(2000 * 10)
        const jobs = []

        const mappingOptions = {
          limit: 100,
          offset: 0,
          debug: true,
          type: 'mapping',
          input: '/tmp/mapping.json',
          output: baseUrl + '/file_destination_index'
        }

        const dataOptions = {
          limit: 100,
          offset: 0,
          debug: true,
          type: 'data',
          input: '/tmp/data.json',
          output: baseUrl + '/file_destination_index'
        }

        const mappingDumper = new Elasticdump(mappingOptions.input, mappingOptions.output, mappingOptions)
        const dataDumper = new Elasticdump(dataOptions.input, dataOptions.output, dataOptions)

        mappingDumper.on('error', error => { throw (error) })
        dataDumper.on('error', error => { throw (error) })

        jobs.push(next => { mappingDumper.dump(next) })
        jobs.push(next => { setTimeout(next, 5001) })
        jobs.push(next => { dataDumper.dump(next) })
        jobs.push(next => { setTimeout(next, 5001) })

        async.series(jobs, done)
      })

      it('the dump transfered', done => {
        const url = baseUrl + '/file_destination_index/_search'
        request.get(url, (err, response, body) => {
          should.not.exist(err)
          body = JSON.parse(body)
          // this confirms that there are no orphans too!
          body.hits.total.should.equal(cities.length + (cities.length * people.length))
          done()
        })
      })

      describex('each city should have children', () => {
        cities.forEach(city => {
          it(city + ' should have children', done => {
            const url = baseUrl + '/file_destination_index/_search'
            const payload = {
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

            request.get(url, {body: JSON.stringify(payload)}, (err, response, body) => {
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
