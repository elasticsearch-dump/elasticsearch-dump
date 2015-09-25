var http = require('http');
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

var seed = function(index, type, callback){
  var started = 0;
  for(var key in seeds){
    started++;
    var s = seeds[key];
    s['_uuid'] = key;
    var url = baseUrl + "/" + index + "/" + type + "/" + key;
    request.put(url, {body: JSON.stringify(s)}, function(err, response, body){
      started--;
      if(started == 0){
        request.post(baseUrl + "/" + index + "/_refresh", function(err, response){
          callback();
        });
      }
    });
  }
};

var clear = function(callback){
  request.del(baseUrl + '/destination_index', function(err, response, body){
    request.del(baseUrl + '/source_index', function(err, response, body){
      request.del(baseUrl + '/another_index', function(err, response, body){
        callback();
      });
    });
  });
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
          setTimeout(function(){
            done();
          }, 500);
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

    it('works for index/types in separate option', function(done){
      this.timeout(testTimeout);
      var options = {
        limit:          100,
        offset:         0,
        debug:          false,
        type:           'data',
        input:          baseUrl,
        'input-index':  '/source_index/seeds',
        output:         baseUrl,
        'output-index': '/destination_index',
        scrollTime:     '10m'
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
        scrollTime: '10m'
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
        scrollTime: '10m',
        sourceOnly: false,
        jsonLines: false
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

  describe("es to file jsonLines", function(){
    it('works', function(done){
      this.timeout(testTimeout);
      var options = {
        limit:  100,
        offset: 0,
        debug:  false,
        type:   'data',
        input:  baseUrl + '/source_index',
        output: '/tmp/out.jsonlines',
        scrollTime: '10m',
        sourceOnly: false,
        jsonLines: true
      };

      var dumper = new elasticdump(options.input, options.output, options);

      dumper.dump(function(){
        var raw = fs.readFileSync('/tmp/out.jsonlines').toString();
        var lines = raw.split(/[\r\n]+/g);
        var linecount = lines.length

        // first character should be { not [
        raw[0].should.equal("{")

        // first character of following lines should be { not ,
        lines[1][0].should.equal("{")
        lines[2][0].should.equal("{")

        // one line for each document (500) plus an extra "1" entry for the final \r\n
        linecount.should.equal(501); 

        done();
      });
    });
  });
  
  describe("es to file sourceOnly", function(){
    it('works', function(done){
      this.timeout(testTimeout);
      var options = {
        limit:  100,
        offset: 0,
        debug:  false,
        type:   'data',
        input:  baseUrl + '/source_index',
        output: '/tmp/out.sourceOnly',
        scrollTime: '10m',
        sourceOnly: true,
        jsonLines: false
      };

      var dumper = new elasticdump(options.input, options.output, options);

      dumper.dump(function(){
        var raw = fs.readFileSync('/tmp/out.sourceOnly');
        var output = JSON.parse( raw );
        output.length.should.equal(seedSize);

        // "key" should be immediately available
        output[0]["key"].length.should.be.above(0)
        done();
      });
    });
  });

  describe("es to file jsonLines sourceOnly", function(){
    it('works', function(done){
      this.timeout(testTimeout);
      var options = {
        limit:  100,
        offset: 0,
        debug:  false,
        type:   'data',
        input:  baseUrl + '/source_index',
        output: '/tmp/out.sourceOnly.jsonLines',
        scrollTime: '10m',
        sourceOnly: true,
        jsonLines: true
      };

      var dumper = new elasticdump(options.input, options.output, options);

      dumper.dump(function(){
        var raw = fs.readFileSync('/tmp/out.sourceOnly.jsonLines').toString();
        var lines = raw.split(/[\r\n]+/g);
        var linecount = lines.length

        // first character should be { not [
        raw[0].should.equal("{")

        // first character of following lines should be { not ,
        lines[1][0].should.equal("{")
        lines[2][0].should.equal("{")

        // "key" should be immediately available
        var output = JSON.parse( lines[0] );
        output["key"].length.should.be.above(0)

        // one line for each document (500) plus an extra "1" entry for the final \r\n
        linecount.should.equal(501); 
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
          sourceOnly: false,
          jsonLines: false,
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
