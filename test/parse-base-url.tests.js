var should = require('should') // eslint-disable-line
var parseBaseURL = require('../lib/parse-base-url')

describe('parseBaseURL', function () {
  it('should parse index and type from the input-host', function () {
    parseBaseURL('http://localhost:9200/index/type').should.eql({
      url: 'http://localhost:9200/index/type',
      host: 'http://localhost:9200',
      index: 'index',
      type: 'type'
    })
  })

  it('should parse index from the input-host', function () {
    parseBaseURL('http://localhost:9200/index').should.eql({
      url: 'http://localhost:9200/index',
      host: 'http://localhost:9200',
      index: 'index',
      type: undefined
    })
  })

  it('should not parse index or type from the input-host', function () {
    parseBaseURL('http://localhost:9200').should.eql({
      url: 'http://localhost:9200',
      host: 'http://localhost:9200',
      index: undefined,
      type: undefined
    })
  })

  it('should parse index and type from the input-host with trailing slash', function () {
    parseBaseURL('http://localhost:9200/index/type/').should.eql({
      url: 'http://localhost:9200/index/type',
      host: 'http://localhost:9200',
      index: 'index',
      type: 'type'
    })
  })

  it('should parse index and type from index', function () {
    parseBaseURL('http://localhost:9200/proxied/host', 'index/type').should.eql({
      url: 'http://localhost:9200/proxied/host/index/type',
      host: 'http://localhost:9200/proxied/host',
      index: 'index',
      type: 'type'
    })
  })

  it('should parse index from index', function () {
    parseBaseURL('http://localhost:9200/proxied/host', 'index').should.eql({
      url: 'http://localhost:9200/proxied/host/index',
      host: 'http://localhost:9200/proxied/host',
      index: 'index',
      type: undefined
    })
  })

  it('should parse index and type from index with leading and trailing slashes', function () {
    parseBaseURL('http://localhost:9200/proxied/host', '/index/type/').should.eql({
      url: 'http://localhost:9200/proxied/host/index/type',
      host: 'http://localhost:9200/proxied/host',
      index: 'index',
      type: 'type'
    })
  })

  it('should parse index from index with leading slash', function () {
    parseBaseURL('http://localhost:9200/proxied/host', '/index').should.eql({
      url: 'http://localhost:9200/proxied/host/index',
      host: 'http://localhost:9200/proxied/host',
      index: 'index',
      type: undefined
    })
  })

  it('should accept an empty index parameter to mean url is host', function () {
    parseBaseURL('http://localhost:9200/proxied/host', '/').should.eql({
      url: 'http://localhost:9200/proxied/host',
      host: 'http://localhost:9200/proxied/host',
      index: undefined,
      type: undefined
    })
  })
})
