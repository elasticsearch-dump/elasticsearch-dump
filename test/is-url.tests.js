const { isUrl } = require('../lib/is-url')
var should = require('should') // eslint-disable-line

describe('is-url', () => {
  it('returns true if url starts with http://', () => {
    isUrl('http://blah.com').should.equal(true)
  })
  it('returns true if url starts with https://', () => {
    isUrl('https://blah.com').should.equal(true)
  })
  it('returns false if called with nothing', () => {
    isUrl().should.equal(false)
  })
  it('does not choke when passed something other than string', () => {
    isUrl(1).should.equal(false)
  })
  it('returns false for windows file path', () => {
    isUrl('c:\\some\\windows\\path').should.equal(false)
  })
  it('returns false for unix file path', () => {
    isUrl('/some/unix/path').should.equal(false)
  })
})
