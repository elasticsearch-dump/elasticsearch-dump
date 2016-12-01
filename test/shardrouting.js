"use strict";

/*
    Test suite for shard routing preservation.
 */

var elasticdump = require( __dirname + '/../elasticdump.js' ).elasticdump;
var request     = require('request');
var should      = require('should');
var fs          = require('fs');
var async       = require('async');
var baseUrl     = 'http://127.0.0.1:9200';
var indexes     = ['source_routing_index', 'destination_routing_index'];
var datafile       = ['/tmp/data.json'];
var mapping     = {
  "doc" : {
    "_source" : {
        "enabled" : true
    },
    "properties": {
        "domain": {
            "type": "string",
            "index": "not_analyzed"
        },
        "name": {
            "type": "string",
            "index": "not_analyzed"
        },
    }
  }
};

var clear = function(callback){
  var jobs = [];
  indexes.forEach(function(index){
    jobs.push(function(done){
      request.del(baseUrl + '/' + index, done);
    });
  });

  jobs.push(function(done){
    try{
      fs.unlinkSync(datafile);
    }catch(e){ }
    done();
  });

  async.series(jobs, callback);
};

var setup = function(callback){
  var jobs = [];

  jobs.push(function(done){
    var url = baseUrl + "/source_routing_index";
    var payload = {mappings: mapping};
    request.put(url, {body: JSON.stringify(payload)}, done);
  });

  // create doc with shard _routing
  jobs.push(function(done){
    var url = baseUrl + "/source_routing_index/site/1?routing=www.google.com";
    var payload = {
      name: "Google",
      domain: "www.google.com",
    };
    request.put(url, {body: JSON.stringify(payload)}, done);
  });

  async.series(jobs, callback);
};

describe('shard routing', function(){

  before(function(done){
    this.timeout(15 * 1000);
    clear(function(error){
      if(error){ return done(error); }
      setup(done);
    });
  });

  after(function(done){ clear(done); });

  beforeEach(function(done){
    setTimeout(done, 500);
  });

  describe('ES to ES dump should maintain shard _routing', function(){
    before(function(done){
      this.timeout(2000 * 10);
      var jobs = [];

      var mappingOptions = {
        limit:  100,
        offset: 0,
        debug:  false,
        type:   'mapping',
        input:  baseUrl + '/source_routing_index',
        output: baseUrl + '/destination_routing_index',
        scrollTime: '10m'
      };

      var dataOptions = {
        limit:  100,
        offset: 0,
        debug:  false,
        type:   'data',
        input:  baseUrl + '/source_routing_index',
        output: baseUrl + '/destination_routing_index',
        scrollTime: '10m'
      };

      var mappingDumper = new elasticdump(mappingOptions.input, mappingOptions.output, mappingOptions);
      var dataDumper    = new elasticdump(dataOptions.input,    dataOptions.output,    dataOptions);

      mappingDumper.on('error', function(error){ throw(error); });
      dataDumper.on('error', function(error){ throw(error); });

      jobs.push(function(next){ mappingDumper.dump(next); });
      jobs.push(function(next){ setTimeout(next, 5001) });
      jobs.push(function(next){ dataDumper.dump(next); });
      jobs.push(function(next){ setTimeout(next, 5001) });

      async.series(jobs, done);
    });

    it('the dump transfered', function(done){
      var url = baseUrl + "/destination_routing_index/_search";
      request.get(url, function(err, response, body){
        body = JSON.parse(body);
        body.hits.total.should.equal(1);
        done();
      });
    });

    describe('each doc should have _routing maintained', function(){
      it('doc should have shard _routing maintained', function(done){
        var url = baseUrl + "/destination_routing_index/_search";
        var payload = {
          "query": {
            "wildcard": {
              "name": "*Google*"
            }
          }
        };

        request.get(url, {body: JSON.stringify(payload)}, function(err, response, body){
          body = JSON.parse(body);
          body.hits.total.should.equal(1);
          should.exist( body.hits.hits[0]._routing );
          done();
        });
      });
    });

  });

  describe('ES to File and back to ES should work', function(){
    before(function(done){
      this.timeout(2000 * 10);
      var jobs = [];

      var dataOptions = {
        limit:  100,
        offset: 0,
        debug:  false,
        type:   'data',
        input:  baseUrl + '/source_routing_index',
        output: '/tmp/data.json',
        scrollTime: '10m'
      };

      var dataDumper    = new elasticdump(dataOptions.input,    dataOptions.output,    dataOptions);

      dataDumper.on('error', function(error){ throw(error); });

      jobs.push(function(next){ dataDumper.dump(next); });
      jobs.push(function(next){ setTimeout(next, 5001) });

      async.series(jobs, done);
    });

    it('the dump files should have worked', function(done){
      var data = String( fs.readFileSync('/tmp/data.json') );
      var dataLines = [];

      data.split('\n').forEach(function(line){
        if(line.length > 2){ dataLines.push( JSON.parse(line) ); }
      });

      var dumpedDocs = [];
      dataLines.forEach(function(d){
        should.exist( d._routing );
        dumpedDocs.push(d);
      });

      dumpedDocs.length.should.equal(1);
      done();
    });

  });
});
