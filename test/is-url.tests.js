const { isUrl, encodeURIOnce } = require('../lib/is-url')
require('should')

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
  it('returns true if url is encoded', () => {
    isUrl('http%3A%2F%2Fgithub.com%2Fkuzzleio%2Fkuzzle').should.equal(true)
  })
})

describe('encodeURIOnce', () => {
  const decodedURI = 'http://github.com/kuzzleio/kuzzle'
  const encodedURI = 'http%3A%2F%2Fgithub.com%2Fkuzzleio%2Fkuzzle'

  it('returns encoded URI if not encoded', () => {
    encodeURIOnce(decodedURI).should.equal(encodedURI)
  })
  it('returns same encoded URI if already encoded', () => {
    encodeURIOnce(encodedURI).should.equal(encodedURI)
  })
  it('returns encoded partial URI component', () => {
    encodeURIOnce('%kuzzle').should.equal('%25kuzzle')
  })
  it('returns same encoded partial URI component if already encoded', () => {
    encodeURIOnce('%25kuzzle').should.equal('%25kuzzle')
  })
})
