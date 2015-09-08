var http = require('http');
var assert = require("assert");
var Promise = require("promise");
http.globalAgent.maxSockets = 10;

var elasticdump                = require( __dirname + "/../elasticdump.js" ).elasticdump;
var request                    = require('request');
var should                     = require('should');
var fs                         = require('fs');
var baseUrl                    = "http://127.0.0.1:9200";

var seeds                      = {};
var seedSize                   = 500;
var testTimeout                = seedSize * 100;
var i                          = 0;
var indexesExistingBeforeSuite = 0;

while(i < seedSize){
  seeds[i] = { key: ("key" + i) };
  i++;
}

var es = {
  // { type: "seeds", id: "0" }
  urlFor: function(idx, pathParams) {
    assert(pathParams.type, "type must be provided");
    url = baseUrl + "/" + idx + "/" + pathParams.type;
    if ('id' in pathParams)
      url += "/" + pathParams.id;
    return url;
  },
  get: function(idx, pathParams) {
    return new Promise(function (resolve, reject) {
      request.get(es.urlFor(idx, pathParams), function(err, response, body) {
        if (err === null)
          resolve(JSON.parse(body));
        else
          reject(response);
      });
    });
  },
  post: function(idx, pathParams, body) {
    return new Promise(function(resolve, reject) {
      request.post(es.urlFor(idx, pathParams), {body: JSON.stringify(body)}, function(err, response, body){
        if (err === null)
          resolve(JSON.parse(body));
        else
          reject(response);
      });
    });
  },
  put: function(idx, pathParams, body) {
    return new Promise(function(resolve, reject) {
      request.put(es.urlFor(idx, pathParams), {body: JSON.stringify(body)}, function(err, response, body){
        if (err === null)
          resolve(JSON.parse(body));
        else
          reject(response);
      });
    });
  },
  refresh: function(/*indices...*/) {
    var indices = Array.prototype.slice.call(arguments);
    return new Promise(function(resolve, reject) {
      request.post(baseUrl + "/" + indices.join(",") + "/_refresh", function(err, response) {
        if(err === null)
          resolve();
        else
          reject(response);
      });
    });
  },
  deleteIndex: function(idx) {
    return new Promise(function(resolve, reject) {
      request.del(baseUrl + '/' + idx, function(err, response, body) {
        if (err === null)
          resolve();
        else
          reject(err);
      });
    });
  },
  runDump: function(elasticdump) {
    return new Promise(function(resolve, reject) {
      elasticdump.dump(function(err, total_writes) {
        if (err === null)
          resolve(total_writes);
        else
          reject(err);
      });
    });
  }
};

var seed = function(index, type, callback){
  var puts = [];
  for(var key in seeds){
    var s = seeds[key];
    s['_uuid'] = key;
    puts.push(es.put(index, {type: type, id: key}, s));
  };
  return Promise.all(puts).then(
    function() {
      return es.refresh(index).then(
        callback,
        function(err) { throw err; }
      );
    },
    function(err) { throw err; }
  );
};

var clear = function(callback){
  return Promise.all([
    es.deleteIndex('destination_index'),
    es.deleteIndex('source_index'),
    es.deleteIndex('another_index')
  ]).then(
    callback,
    function(err) { throw err; }
  );
};

describe("ELASTICDUMP", function(){

  before(function(done){
    request(baseUrl + '/_cat/indices', function(err, response, body){
      lines = body.split("\n");
      lines.forEach(function(line){
        words = line.split(' ');
        index = words[2];
        if(line.length > 0 && ['source_index', 'another_index', 'destination_index'].indexOf(index) < 0){ 
          indexesExistingBeforeSuite++; 
        }
      });
      done();
    });
  });

  beforeEach(function(done){
    this.timeout(testTimeout);
    clear(function(){
      seed("source_index", 'seeds', function(){
        seed("another_index", 'seeds', function(){
          es.refresh("source_index", "another_index").then(done);
        });
      });
    });
  });

  it('can connect', function(done){
    this.timeout(testTimeout);
    request(baseUrl, function(err, response, body){
      should.not.exist(err);
      body = JSON.parse(body);
      body.tagline.should.equal('You Know, for Search');
      done();
    });
  });

  it('source_index starts filled', function(done){
    this.timeout(testTimeout);
    var url = baseUrl + "/source_index/_search";
    request.get(url, function(err, response, body){
      body = JSON.parse(body);
      body.hits.total.should.equal(seedSize);
      done();
    });
  });

  it('destination_index starts non-existant', function(done){
    this.timeout(testTimeout);
    var url = baseUrl + "/destination_index/_search";
    request.get(url, function(err, response, body){
      body = JSON.parse(body);
      body.status.should.equal(404);
      done();
    });
  });

  describe("es to es", function(){
    it('works for a whole index', function(done){
      this.timeout(testTimeout);
      var options = {
        limit:  100,
        offset: 0,
        debug:  false,
        type:   'data',
        input:  baseUrl + '/source_index',
        output: baseUrl + '/destination_index',
        scrollTime: '10m'
      };

      var dumper = new elasticdump(options.input, options.output, options);

      dumper.dump(function(){
        var url = baseUrl + "/destination_index/_search";
        request.get(url, function(err, response, body){
          should.not.exist(err);
          body = JSON.parse(body);
          body.hits.total.should.equal(seedSize);
          done();
        });
      });
    });

    it('can skip', function(done) {
      this.timeout(testTimeout);
      var options = {
        limit:  100,
        offset: 0,
        debug:  false,
        type:   'data',
        input:  baseUrl + '/source_index',
        output: baseUrl + '/destination_index',
        scrollTime: '10m',
        skip: 250
      };

      var dumper = new elasticdump(options.input, options.output, options);

      dumper.dump(function(){
        var url = baseUrl + "/destination_index/_search";
        request.get(url, function(err, response, body){
          should.not.exist(err);
          body = JSON.parse(body);
          body.hits.total.should.equal(seedSize - 250);
          done();
        });
      });
    });

    it('works for index/types', function(done){
      this.timeout(testTimeout);
      var options = {
        limit:  100,
        offset: 0,
        debug:  false,
        type:   'data',
        input:  baseUrl + '/source_index/seeds',
        output: baseUrl + '/destination_index',
        scrollTime: '10m'
      };

      var dumper = new elasticdump(options.input, options.output, options);

      dumper.dump(function(){
        var url = baseUrl + "/destination_index/_search";
        request.get(url, function(err, response, body){
          should.not.exist(err);
          body = JSON.parse(body);
          body.hits.total.should.equal(seedSize);
          done();
        });
      });
    });

    it('works with searchBody', function(done){
      this.timeout(testTimeout);
      var options = {
        limit:  100,
        offset: 0,
        debug:  false,
        type:   'data',
        input:  baseUrl + '/source_index/seeds',
        output: baseUrl + '/destination_index',
        scrollTime: '10m',
        searchBody: {"query": { "term": { "key": "key1"} } }
      };

      var dumper = new elasticdump(options.input, options.output, options);

      dumper.dump(function(){
        var url = baseUrl + "/destination_index/_search";
        request.get(url, function(err, response, body){
          should.not.exist(err);
          body = JSON.parse(body);
          body.hits.total.should.equal(1);
          done();
        });
      });
    });

    it('works with searchBody range', function(done){
      // Test Note: Since UUID is ordered as string, lte: 2 should return _uuids 0,1,2,    10-19,  100-199 for a total of 113
      this.timeout(testTimeout);
      var options = {
        limit:  100,
        offset: 0,
        debug:  false,
        type:   'data',
        input:  baseUrl + '/source_index/seeds',
        output: baseUrl + '/destination_index',
        scrollTime: '10m',
        searchBody: {"query": {"range": { "_uuid": { "lte": "2"} } }}
      };

      var dumper = new elasticdump(options.input, options.output, options);

      dumper.dump(function(){
        var url = baseUrl + "/destination_index/_search";
        request.get(url, function(err, response, body){
          should.not.exist(err);
          body = JSON.parse(body);
          body.hits.total.should.equal(113);
          done();
        });
      });
    });

    it('can get and set mapping', function(done){
      this.timeout(testTimeout);
      var options = {
        limit:  100,
        offset: 0,
        debug:  false,
        type:   'mapping',
        input:  baseUrl + '/source_index',
        output: baseUrl + '/destination_index',
        scrollTime: '10m'
      };

      var dumper = new elasticdump(options.input, options.output, options);

      dumper.dump(function(){
        var url = baseUrl + "/destination_index/_search";
        request.get(url, function(err, response, body){
          should.not.exist(err);
          body = JSON.parse(body);
          body.hits.total.should.equal(0);
          var url = baseUrl + "/destination_index/_mapping";
          request.get(url, function(err, response, body){
            body = JSON.parse(body);
            body.destination_index.mappings.seeds.properties.key.type.should.equal('string');
            done();
          });
        });
      });
    });

    it('works with a small limit', function(done){
      this.timeout(testTimeout);
      var options = {
        limit:  10,
        offset: 0,
        debug:  false,
        type:   'data',
        input:  baseUrl + '/source_index',
        output: baseUrl + '/destination_index',
        scrollTime: '10m'
      };

      var dumper = new elasticdump(options.input, options.output, options);

      dumper.dump(function(){
        var url = baseUrl + "/destination_index/_search";
        request.get(url, function(err, response, body){
          should.not.exist(err);
          body = JSON.parse(body);
          body.hits.total.should.equal(seedSize);
          done();
        });
      });
    });

    it('works with a large limit', function(done){
      this.timeout(testTimeout);
      var options = {
        limit:  9999999,
        offset: 0,
        debug:  false,
        type:   'data',
        input:  baseUrl + '/source_index',
        output: baseUrl + '/destination_index',
        scrollTime: '10m'
      };

      var dumper = new elasticdump(options.input, options.output, options);

      dumper.dump(function(){
        var url = baseUrl + "/destination_index/_search";
        request.get(url, function(err, response, body){
          should.not.exist(err);
          body = JSON.parse(body);
          body.hits.total.should.equal(seedSize);
          done();
        });
      });
    });

    it('counts updates as writes', function(done){
      this.timeout(testTimeout);
      var options = {
        limit:  100,
        offset: 0,
        debug:  false,
        type:   'data',
        input:  baseUrl + '/source_index',
        output: baseUrl + '/destination_index',
        scrollTime: '10m',
        update: true
      };

      var dumper_a = new elasticdump(options.input, options.output, options);
      var dumper_b = new elasticdump(options.input, options.output, options);

      dumper_a.dump(function(err, total_writes){
        var url = baseUrl + "/destination_index/_search";
        request.get(url, function(err, response, body){
          should.not.exist(err);
          body = JSON.parse(body);
          body.hits.total.should.equal(seedSize);
          total_writes.should.equal(seedSize);

          dumper_b.dump(function(err, total_writes){
            var url = baseUrl + "/destination_index/_search";
            request.get(url, function(err, response, body){
              should.not.exist(err);
              body = JSON.parse(body);
              body.hits.total.should.equal(seedSize);
              total_writes.should.equal(seedSize);
              done();
            });
          });

        });
      });
    });

    describe("with update: false", function() {
      [
        {bulk: true,  desc: "when bulk updating"},
        {bulk: false, desc: "when non-bulk updating"}
      ].forEach(function(o) {
        it('does not update existing documents ' + o.desc, function(done) {
          this.timeout(testTimeout);
          var options = {
            limit:  1000,
            offset: 0,
            debug:  false,
            type:   'data',
            input:  baseUrl + '/source_index',
            output: baseUrl + '/another_index',
            scrollTime: '10m',
            bulk: o.bulk,
            'bulk-use-output-index-name': o.bulk,
            update: false
          };

          Promise.
            all([
              es.put ("source_index", {type: "seeds", id: "0"},    {"key": "updated", "_uuid": "0"}),
              es.post("source_index", {type: "seeds", id: "9999"}, {"key": "new",     "_uuid": "9999"})
            ]).
            then(function() {
              return es.refresh("source_index");
            }).
            then(function() {
              return es.runDump(new elasticdump(options.input, options.output, options));
            }).
            then(function(total_writes) {
              return es.refresh("another_index");
            }).
            then(function() {
              return Promise.all([
                es.get("another_index", {type: "seeds", id: "0"}),
                es.get("another_index", {type: "seeds", id: "9999"})
              ]);
            }).
            then(function(results) {
              results[0]._source["key"].should.equal("key0"); // not updated
              results[0]._source["_uuid"].should.equal("0");

              results[1]._source["key"].should.equal("new");
              results[1]._source["_uuid"].should.equal("9999");
            }).
            then(done, done);
        });
      });
    });

    it('can also delete documents from the source index', function(done){
      this.timeout(testTimeout);
      var options = {
        limit:  100,
        offset: 0,
        debug:  false,
        type:   'data',
        delete: true,
        input:  baseUrl + '/source_index',
        output: baseUrl + '/destination_index',
        scrollTime: '10m'
      };

      var dumper = new elasticdump(options.input, options.output, options);

      dumper.dump(function(){
        var url = baseUrl + "/destination_index/_search";
        request.get(url, function(err, response, destination_body){
          destination_body = JSON.parse(destination_body);
          destination_body.hits.total.should.equal(seedSize);
          dumper.input.reindex(function(){
            // Note: Depending on the speed of your ES server
            // all the elements might not be deleted when the HTTP response returns
            // sleeping is required, but the duration is based on your CPU, disk, etc.
            // lets guess 1ms per entry in the index
            setTimeout(function(){
              var url = baseUrl + "/source_index/_search";
              request.get(url, function(err, response, source_body){
                source_body = JSON.parse(source_body);
                source_body.hits.total.should.equal(0);
                done();
              });
            }, 5 * seedSize);
          });
        });
      });
    });
  });

  describe("es to file", function(){
    it('works', function(done){
      this.timeout(testTimeout);
      var options = {
        limit:  100,
        offset: 0,
        debug:  false,
        type:   'data',
        input:  baseUrl + '/source_index',
        output: '/tmp/out.json',
        scrollTime: '10m'
      };

      var dumper = new elasticdump(options.input, options.output, options);

      dumper.dump(function(){
        var raw = fs.readFileSync('/tmp/out.json');
        var output = JSON.parse( raw );
        output.length.should.equal(seedSize);
        done();
      });
    });
  });

  describe("file to es", function(){
    it('works', function(done){
      this.timeout(testTimeout);
      var options = {
        limit:  100,
        offset: 0,
        debug:  false,
        type:   'data',
        input: '/tmp/out.json',
        output: baseUrl + '/destination_index',
        scrollTime: '10m'
      };

      var dumper = new elasticdump(options.input, options.output, options);

      dumper.dump(function(){
        var url = baseUrl + "/destination_index/_search";
        request.get(url, function(err, response, body){
          should.not.exist(err);
          body = JSON.parse(body);
          body.hits.total.should.equal(seedSize);
          done();
        });
      });
    });

    it('can skip', function(done) {
      this.timeout(testTimeout);
      var options = {
        limit:  100,
        offset: 0,
        debug:  false,
        type:   'data',
        input: '/tmp/out.json',
        output: baseUrl + '/destination_index',
        scrollTime: '10m',
        skip: 250
      };

      var dumper = new elasticdump(options.input, options.output, options);

      dumper.dump(function(){
        var url = baseUrl + "/destination_index/_search";
        request.get(url, function(err, response, body){
          should.not.exist(err);
          body = JSON.parse(body);
          // skips 250 so 250 less in there
          body.hits.total.should.equal(seedSize - 250);
          done();
        });
      });
    });
  });

  describe("all es to file", function(){

    it('works', function(done){
      if(indexesExistingBeforeSuite > 0){
        console.log('');
        console.log('! ' + indexesExistingBeforeSuite + ' ES indeses detected');
        console.log('! Skipping this test as your ES cluster has more indexes than just the test indexes');
        console.log('! Please empty your local elasticsearch and retry this test');
        console.log('');
        done();
      }else{
        this.timeout(testTimeout);
        var options = {
          limit:  100,
          offset: 0,
          debug:  false,
          type:   'data',
          input:  baseUrl,
          output: '/tmp/out.json',
          scrollTime: '10m',
          all:    true
        };

        var dumper = new elasticdump(options.input, options.output, options);

        dumper.dump(function(){
          var raw = fs.readFileSync('/tmp/out.json');
          var output = JSON.parse( raw );
          count = 0;
          for(var i in output){
            var elem = output[i];
            if(elem['_index'] === 'source_index' || elem['_index'] === 'another_index'){
              count++;
            }
          }

          count.should.equal(seedSize * 2);
          done();
        });
      }
    });
  });

  describe("file to bulk es", function(){
    it('works', function(done){
      this.timeout(testTimeout);
      var options = {
        limit:  100,
        offset: 0,
        debug:  false,
        type:   'data',
        output:  baseUrl,
        input: __dirname + '/seeds.json',
        all:    true,
        bulk:   true,
        scrollTime: '10m'
      };

      var dumper = new elasticdump(options.input, options.output, options);

      clear(function(){
        dumper.dump(function(){
          request.get(baseUrl + "/source_index/_search", function(err, response, body1){
            request.get(baseUrl + "/another_index/_search", function(err, response, body2){
              body1 = JSON.parse(body1);
              body2 = JSON.parse(body2);
              body1.hits.total.should.equal(5);
              body2.hits.total.should.equal(5);
              done();
            });
          });
        });
      });
    });
  });

  describe("file to bulk es, respecting output name", function(){
    it('works', function(done){
      this.timeout(testTimeout);
      var options = {
        limit:  100,
        offset: 0,
        debug:  false,
        type:   'data',
        output: baseUrl + '/destination_index',
        input: __dirname + '/seeds.json',
        all:    true,
        bulk:   true,
        scrollTime: '10m',
        'bulk-use-output-index-name': true
      };

      var dumper = new elasticdump(options.input, options.output, options);

      clear(function(){
        dumper.dump(function(){
          request.get(baseUrl + "/destination_index/_search", function(err, response, body){
            body = JSON.parse(body);
            body.hits.total.should.equal(10);
            done();
          });
        });
      });
    });
  });

  describe("es to stdout", function(){
    it('works');
  });

  describe("stdin to es", function(){
    it('works');
  });

});
