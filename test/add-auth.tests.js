var addAuth = require('../lib/add-auth')
var should = require('should') // eslint-disable-line

describe('add-auth', function () {
  it('should throw ENOENT if the auth file is missing', function () {
    (function () {
      addAuth('http://blah.com', 'NotAFile.txt')
    }).should.throw('ENOENT: no such file or directory, open \'NotAFile.txt\'')
  })
  it('should throw error if the auth file is missing username and password', function () {
    (function () {
      addAuth('http://blah.com', 'test/test-resources/malformedHttpAuth.ini')
    }).should.throw('Malformed Auth File')
  })
  it('shouldn\'t overwrite existing auth parameters in url', function () {
    addAuth('http://user:pass@blah.com', 'test/test-resources/httpAuthTest.ini').should.equal('http://user:pass@blah.com')
  })
  it('should add auth parameters if they are missing', function () {
    addAuth('http://blah.com', 'test/test-resources/httpAuthTest.ini').should.equal('http://foo:bar@blah.com/')
  })
})
