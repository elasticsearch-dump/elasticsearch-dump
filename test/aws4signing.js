var should = require('should');
var aws4signer = require('../lib/aws4signer');
var parent = {options: {awsAccessKeyId: 'key', awsSecretAccessKey: 'secret'}}

describe('aws4signer', function () {
	it('should parse "uri" from request object and add signature, if credentials provided', function() {
		var r = {
			uri : 'http://127.0.0.1:9200/_search?q=test',
			method : 'GET',
			body: '{"query": { "match_all": {} }, "fields": ["*"], "_source": true }'
		};

		aws4signer(r, parent);

		r.should.have.property('headers').which.is.an.Object;
		r.headers.should.have.property('host').which.is.a.String();
		r.headers.should.have.property('Content-Type').which.is.a.String();
		r.headers.should.have.property('Content-Length').which.is.a.Number();
		r.headers.should.have.property('X-Amz-Date').which.is.a.String();
		r.headers.should.have.property('Authorization').which.containEql('Credential');
		r.headers.should.have.property('Authorization').which.containEql('SignedHeaders');
		r.should.have.property('path');
		r.should.have.property('hostname');
	});

	it('should parse "url" from request object and add signature, if credentials provided', function() {
		var r = {
			url : 'http://127.0.0.1:9200/_search?q=test',
			method : 'GET',
			body: '{"query": { "match_all": {} }, "fields": ["*"], "_source": true }'
		};
		aws4signer(r, parent);

		r.should.have.property('headers').which.is.an.Object;
		r.headers.should.have.property('host').which.is.a.String();
		r.headers.should.have.property('Content-Type').which.is.a.String();
		r.headers.should.have.property('Content-Length').which.is.a.Number();
		r.headers.should.have.property('X-Amz-Date').which.is.a.String();
		r.headers.should.have.property('Authorization').which.containEql('Credential');
		r.headers.should.have.property('Authorization').which.containEql('SignedHeaders');
		r.should.have.property('path');
		r.should.have.property('hostname');
	});

	it('should not add signature if credential (key, secret) is NOT provided', function() {
		var r = {
			uri : 'http://127.0.0.1:9200/_search?q=test',
			method : 'GET',
			body: '{"query": { "match_all": {} }, "fields": ["*"], "_source": true }'
		};
		aws4signer(r, {options: {}});

		if(r.headers !== undefined) {
			r.headers.should.not.have.property('X-Amz-Date');
			r.headers.should.not.have.property('Authorization');
		}
	});

	it('should not add signature if credential (secret or key) is NOT provided', function() {
		var r = {
			uri : 'http://127.0.0.1:9200/_search?q=test',
			method : 'GET',
			body: '{"query": { "match_all": {} }, "fields": ["*"], "_source": true }'
		};
		aws4signer(r, {options: {something: 'else'}});

		if(r.headers !== undefined) {
			r.headers.should.not.have.property('X-Amz-Date');
			r.headers.should.not.have.property('Authorization');
		}
	});
});
