var sut = require('../lib/is-url')
var should = require('should') // eslint-disable-line

describe('is-url', function () {
  it('returns true if url starts with http://', function () {
    sut('http://blah.com').should.equal(true)
  })
  it('returns true if url starts with https://', function () {
    sut('https://blah.com').should.equal(true)
  })
  it('returns false if called with nothing', function () {
    sut().should.equal(false)
  })
  it('does not choke when passed something other than string', function () {
    sut(1).should.equal(false)
  })
  it('returns false for windows file path', function () {
    sut('c:\\some\\windows\\path').should.equal(false)
  })
  it('returns false for unix file path', function () {
    sut('/some/unix/path').should.equal(false)
  })
})
