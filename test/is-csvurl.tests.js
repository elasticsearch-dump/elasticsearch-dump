const { fromCsvUrl, isCsvUrl } = require('../lib/is-url')
var should = require('should') // eslint-disable-line

describe('fromCsvUrl & isCsvUrl', function () {
  it('returns true if url starts with csv://', function () {
    isCsvUrl('csv:///home/ferron/csv').should.equal(true)
  })
  it('returns false if called with nothing', function () {
    isCsvUrl().should.equal(false)
  })
  it('does not choke when passed something other than string', function () {
    isCsvUrl(1).should.equal(false)
  })
  it('returns true for windows file path', function () {
    isCsvUrl('csv://c:\\some\\windows\\path').should.equal(true)
  })
  it('returns false for unix file path', function () {
    isCsvUrl('csv:///some/unix/path').should.equal(true)
  })
  it('returns true for windows file path', function () {
    fromCsvUrl('csv://c:\\some\\windows\\path').should.deepEqual({ protocol: 'csv://', path: 'c:\\some\\windows\\path' })
  })
})
