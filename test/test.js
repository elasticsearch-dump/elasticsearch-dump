var elasticdump = require( __dirname + "/../elasticdump.js" ).elasticdump;
var request     = require('request');
var should      = require('should');
var fs          = require('fs');
var baseUrl     = "http://127.0.0.1:9200";

var seeds = {
  '0000000000000000000000000000000000000001': {key: 'key1'},
  '0000000000000000000000000000000000000002': {key: 'key2'},
  '0000000000000000000000000000000000000003': {key: 'key3'},
  '0000000000000000000000000000000000000004': {key: 'key4'},
  '0000000000000000000000000000000000000005': {key: 'key5'},
  '0000000000000000000000000000000000000006': {key: 'key6'},
  '0000000000000000000000000000000000000007': {key: 'key7'},
  '0000000000000000000000000000000000000008': {key: 'key8'},
  '0000000000000000000000000000000000000009': {key: 'key9'},
  '0000000000000000000000000000000000000010': {key: 'key10'},
}

var seed = function(callback){
  var started = 0;
  for(var key in seeds){
    started++;
    var seed = seeds[key];
    seed['_uuid'] = key;
    var url = baseUrl + "/source_index/seeds/" + key;
    request.put(url, {body: JSON.stringify(seed)}, function(err, response, body){
      started--;
      if(started == 0){
        request.post(baseUrl + "/source_index/_refresh", function(err, response){
          callback();
        });
      }
    });
  }
}

var clear = function(callback){
  request.del(baseUrl + '/destination_index', function(err, response, body){
    request.del(baseUrl + '/source_index', function(err, response, body){
      callback();
    });
  });
}

describe("ELASTICDUMP", function(){

  beforeEach(function(done){
    this.timeout = 10 * 1000;
    clear(function(){
      seed(function(){
        setTimeout(function(){
          done();
        }, 500);
      });
    });
  });

  it('can connect', function(done){
    this.timeout = 10 * 1000;
    request(baseUrl, function(err, response, body){
      should.not.exist(err);
      body = JSON.parse(body);
      body.tagline.should.equal('You Know, for Search');
      done();
    })
  }); 

  describe("es to es", function(){
    it('works', function(done){
      this.timeout = 10 * 1000;
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
          body.hits.hits.length.should.equal(10);
          done();
        });
      });
    });
  });

  describe("es to file", function(){
    it('works', function(done){
      this.timeout = 10 * 1000;
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
        output.length.should.equal(10);
        done();
      });
    });
  });

  describe("file to es", function(){
    it('works', function(done){
      this.timeout = 10 * 1000;
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
          body.hits.hits.length.should.equal(10);
          done();
        });
      });

    });
  });

});