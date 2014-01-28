var elasticdump = require( __dirname + "/../elasticdump.js" ).elasticdump;
var request     = require('request');
var should      = require('should');
var fs          = require('fs');
var baseUrl     = "http://127.0.0.1:9200";

var seeds       = {};
var seedSize    = 500;
var testTimeout = seedSize * 100;
var i           = 0;
while(i < seedSize){
  seeds[i] = { key: ("key" + i) };
  i++;
}

var seed = function(index, callback){
  var started = 0;
  for(var key in seeds){
    started++;
    var seed = seeds[key];
    seed['_uuid'] = key;
    var url = baseUrl + "/" + index + "/seeds/" + key;
    request.put(url, {body: JSON.stringify(seed)}, function(err, response, body){
      started--;
      if(started == 0){
        request.post(baseUrl + "/" + index + "/_refresh", function(err, response){
          callback();
        });
      }
    });
  }
}

var clear = function(callback){
  request.del(baseUrl + '/destination_index', function(err, response, body){
    request.del(baseUrl + '/source_index', function(err, response, body){
      request.del(baseUrl + '/another_index', function(err, response, body){
        callback();
      });
    });
  });
}

describe("ELASTICDUMP", function(){

  beforeEach(function(done){
    this.timeout(testTimeout);
    clear(function(){
      seed("source_index", function(){
        seed("another_index", function(){
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
    })
  }); 

  it('source_index starts filled', function(done){
    this.timeout(testTimeout);
    var url = baseUrl + "/source_index/_search"
    request.get(url, function(err, response, body){
      body = JSON.parse(body);
      body.hits.total.should.equal(seedSize);
      done();
    });
  });

  it('destination_index starts non-existant', function(done){
    this.timeout(testTimeout);
    var url = baseUrl + "/destination_index/_search"
    request.get(url, function(err, response, body){
      body = JSON.parse(body);
      body.status.should.equal(404);
      done();
    });
  });

  describe("es to es", function(){
    it('works', function(done){
      this.timeout(testTimeout);
      var options = {
        limit:  100,
        offset: 0,
        debug:  false,
        input:  baseUrl + '/source_index',
        output: baseUrl + '/destination_index',
      }

      var dumper = new elasticdump(options.input, options.output, options);

      dumper.dump(function(){
        var url = baseUrl + "/destination_index/_search"
        request.get(url, function(err, response, body){
          should.not.exist(err);
          body = JSON.parse(body);
          body.hits.total.should.equal(seedSize);
          done();
        });
      });
    });

    it('can also delete documents from the source index', function(done){
      this.timeout(testTimeout);
      var options = {
        limit:  100,
        offset: 0,
        debug:  false,
        delete: true,
        input:  baseUrl + '/source_index',
        output: baseUrl + '/destination_index',
      }

      var dumper = new elasticdump(options.input, options.output, options);

      dumper.dump(function(){
        var url = baseUrl + "/destination_index/_search"
        request.get(url, function(err, response, destination_body){
          destination_body = JSON.parse(destination_body);
          destination_body.hits.total.should.equal(seedSize);
          dumper.input.reindex(function(){
            // Note: Depending on the speed of your ES server
            // all the elements might not be deleted when the HTTP response returns
            // sleeping is required, but the duration is based on your CPU, disk, etc.
            // lets guess 1ms per entry in the index
            setTimeout(function(){
              var url = baseUrl + "/source_index/_search"
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
        input:  baseUrl + '/source_index',
        output: '/tmp/out.json',
      }

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
        input: '/tmp/out.json',
        output: baseUrl + '/destination_index',
      }

      var dumper = new elasticdump(options.input, options.output, options);

      dumper.dump(function(){
        var url = baseUrl + "/destination_index/_search"
        request.get(url, function(err, response, body){
          should.not.exist(err);
          body = JSON.parse(body);
          body.hits.total.should.equal(seedSize);
          done();
        });
      });
    });
  });

  describe("all es to file", function(){
    it('works', function(done){
      this.timeout(testTimeout);
      var options = {
        limit:  100,
        offset: 0,
        debug:  false,
        input:  baseUrl,
        output: '/tmp/out.json',
        all:    true,
      }

      var dumper = new elasticdump(options.input, options.output, options);

      dumper.dump(function(){
        var raw = fs.readFileSync('/tmp/out.json');
        var output = JSON.parse( raw );
        output.length.should.equal(seedSize * 2);
        done();
      });
    });
  });

  describe("file to bulk es", function(){
    it('works', function(done){
      this.timeout(testTimeout);
      var options = {
        limit:  100,
        offset: 0,
        debug:  false,
        input:  baseUrl,
        output: '/tmp/out.json',
        all:    true,
        bulk:   true
      }

      var dumper = new elasticdump(options.input, options.output, options);

      dumper.dump(function(){
        var raw = fs.readFileSync('/tmp/out.json');
        var output = JSON.parse( raw );
        output.length.should.equal(seedSize * 2);
        done();
      });
    });
  });

});
