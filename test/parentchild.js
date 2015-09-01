"use strict";

/* Test suite for parent-child relationships
 *
 * Parent-Child relations demand a mapping, otherwise the indexation
 * is rejected with an error.
 */

var http = require('http');
http.globalAgent.maxSockets = 9;

var elasticdump                = require( __dirname + '/../elasticdump.js' ).elasticdump;
var request                    = require('request');
var should                     = require('should');
var fs                         = require('fs');
var baseUrl                    = 'http://127.0.0.1:9200/';

var pcInitElementCount         = 250;
var pcInitIndexName            = 'esdump-pcinit';
var pcCopyIndexName            = 'esdump-pccopy';
var pcRestoreIndexName         = 'esdump-pcrestored';
var pcDumpMappingFile          = '/tmp/esdump.mapping.json'
var pcDumpDataFile             = '/tmp/esdump.data.json'
var pcInitIndexSettings        = {
  index: {
    number_of_shard: 2,
    number_of_replicas: 0,
    refresh: '1s'
  }
};
var pcInitIndexMapping         = {
  units: {
    dynamic: 'strict',
    properties: {},
    _parent: {
      type: 'tens'
    },
    _routing: {
      required: true
    }
  },
  tens: {
    dynamic: 'strict',
    properties: {}
  }
};

var testTimeout                = pcInitElementCount * 100;

/////////////// Suite ////////////////////////////
describe('Parent-Child Test Suite', pcSuite);

function pcSuite() {
  describe('Initial index creation', function() {
    before(pcInit);
    after(pcCleanup);

    it('exists', testIndexExists(pcInitIndexName));
    it('has correct mappings', testIndexMappings(pcInitIndexName));
    it('has correct # of elements', testIndexElements(pcInitIndexName));
    it('has no orphans', testIndexNoOrphans(pcInitIndexName));
  });

  describe('Copy from ES to ES', function() {
    before(pcInit);
    after(pcCleanup);

    it('can copy a whole index mappings',
       testIndexCopy(pcInitIndexName, pcCopyIndexName, 'mapping'));

    it('dest. exists',
       testIndexExists(pcCopyIndexName));
    it('dest. has correct mappings',
       testIndexMappings(pcCopyIndexName));

    it('can copy a whole index data',
       testIndexCopy(pcInitIndexName, pcCopyIndexName, 'data'));

    it('dest. has correct # of elements',
       testIndexElements(pcCopyIndexName));
    it('dest. has no orphans',
       testIndexNoOrphans(pcCopyIndexName));
  });

  describe('Dump from ES to a file, then restore', function() {
    before(pcInit);
    after(pcCleanup);

    describe('mappings', function() {
      it('can dump an index mappings to a file',
         testDumpIndex(pcInitIndexName, pcDumpMappingFile, 'mapping'));
      it('can restore an index mappings from a file',
         testRestoreIndex(pcDumpMappingFile, pcRestoreIndexName, 'mapping'));

      it('restored index exists',
         testIndexExists(pcRestoreIndexName));
      it('restored index has correct mappings',
         testIndexMappings(pcRestoreIndexName));
    });

    describe('data', function() {
      it('can dump an index data to a file',
         testDumpIndex(pcInitIndexName, pcDumpMappingFile, 'data'));
      it('can restore an index data from a file',
         testRestoreIndex(pcDumpMappingFile, pcRestoreIndexName, 'data'));

      it('restored index has correct # of elements',
         testIndexElements(pcRestoreIndexName));
      it('restored index has no orphans',
         testIndexNoOrphans(pcRestoreIndexName));
    });
  });
}

/////////////// Hooks ////////////////////////////
function pcInit(done) {
  this.timeout(testTimeout);
  var exist = false;
  var setExist = function(bool) { exist = bool; }
  var continuation = function(done) {
    pathMustExists(pcInitIndexName + '/_refresh', false, done);
  };
  var fillIndex = function(done) {
    var counter = {
      count: pcInitElementCount,
      dec: function() { return (--(this.count)); }
    };
    for (var i=0; i<pcInitElementCount; i++) {
      insertElement(pcInitIndexName, i, done, counter, continuation);
    }
  }

  pathExists(pcInitIndexName, setExist, done);

  if (exist)
    return done(new Error(" Index '" + pcInitIndexName + "' already exists"));

  putMappingThen(pcInitIndexName,
                 { mappings: pcInitIndexMapping,
                   settings: pcInitIndexSettings },
                 done,
                 fillIndex);
}

function pcCleanup(done) {
  this.timeout(testTimeout);
  request.del(baseUrl + pcInitIndexName,
              function(err, resp, body) {
                request.del(baseUrl + pcCopyIndexName,
                            function(err, resp, body) {
                              request.del(baseUrl + pcRestoreIndexName,
                                          function(err, resp, body) {
                                            done();
                                          });
                            });
              });
  fs.unlink(pcDumpMappingFile, function(ex) {});
  fs.unlink(pcDumpDataFile, function(ex) {});
}

/////////////// Tests ////////////////////////////
function testIndexExists(index) {
  return (function(done) {
    this.timeout(testTimeout);
    pathMustExists(index, false, done);
  });
}

function testIndexMappings(index) {
  var checkMapping = function(body, done) {
    should.deepEqual(body[index], { mappings: pcInitIndexMapping});
    done();
  }
  return (function(done) {
    this.timeout(testTimeout);
    pathMustExists(index + '/_mapping', checkMapping, done);
  });
}

function testIndexElements(index) {
  var checkElements = function(body, done) {
    should.equal(body.hits.total, pcInitElementCount);
    done();
  }
  return (function(done) {
    this.timeout(testTimeout);
    pathMustExists(index + '/_search?search_type=count', checkElements, done);
  });
}

function testIndexNoOrphans(index) {
  var checkCount = function(body, done) {
    should.equal(body.hits.total, 0);
    done();
  }
  return (function(done) {
    this.timeout(testTimeout);
    esCountOrphans(index, 'units', 'tens', checkCount, done);
  });
}

function testIndexCopy(src, dst, type) {
  var options = {
    limit:  100,
    offset: 0,
    debug:  true,
    type:   type,
    input:  baseUrl + src,
    output: baseUrl + dst,
    scrollTime: '10m'
  };
  var dumper = new elasticdump(options.input, options.output, options);

  return (function(done) {
    this.timeout(testTimeout);
    dumper.dump(function(err, writes) {
      should.not.exists(err);
      pathMustExists(dst + '/_refresh', false, done);
    });
  });
}

function testDumpIndex(index, file, type) {
  var options = {
    limit:  100,
    offset: 0,
    debug:  true,
    type:   type,
    input:  baseUrl + index,
    output: file,
    scrollTime: '10m'
  };
  var dumper = new elasticdump(options.input, options.output, options);

  return (function(done) {
    this.timeout(testTimeout);
    dumper.dump(function(err, writes) {
      should.not.exists(err);
      done();
    });
  });
}

function testRestoreIndex(file, index, type) {
  var options = {
    limit:  100,
    offset: 0,
    debug:  true,
    type:   type,
    input:  file,
    output: baseUrl + index,
    scrollTime: '10m'
  };
  var dumper = new elasticdump(options.input, options.output, options);

  return (function(done) {
    this.timeout(testTimeout);
    dumper.dump(function(err, writes) {
      should.not.exists(err);
      pathMustExists(index + '/_refresh', false, done);
    });
  });
}

/////////////// Utils ////////////////////////////
function requestCont(callback, done) {
  return function(err, resp, body) {
    if (err) return done(err);
    callback(resp, body, done);
  }
}

function pathMustExists(path, callback, done) {
  var exists = function(resp, body, done) {
    if (resp.statusCode == 200) {
      if (callback)
        callback(JSON.parse(body), done);
      else
        done();
    } else if (resp.statusCode == 404) {
      done(new Error("Path '" + path + "' does not exist."));
    } else {
      done(new Error(body));
    }
  }
  request.get(baseUrl + '/' + path, requestCont(exists, done));
}

function pathExists(path, callback, done) {
  var exists = function(resp, body, done) {
    if (resp.statusCode == 200) {
      callback(true);
    } else if (resp.statusCode == 404) {
      callback(false);
    } else {
      console.log("pathExists for '" + path + "' returned " + resp.statusCode);
      done(body);
    }
  }
  request.get(baseUrl + '/' + path, requestCont(exists, done));
}

function putMappingThen(path, mapping, done, then) {
  request.put(baseUrl + path,
              { body: JSON.stringify(mapping) },
              function(err, resp, body) {
                if (err) return done(err);
                if (resp.statusCode == 400) {
                  return done(new Error("Put Mapping failed for '" + path + "': " + body));
                }
                then(done);
              });
}

function insertElement(index, element, done, counter, continuation) {
  var routing = '', parent, type;
  var putURI, putParams = '';

  if (element % 10 == 0) {
    type = 'tens';
  } else {
    type = 'units';
    parent = parseInt(Math.floor(element/10)*10);
    routing = parent;
  }

  putURI = baseUrl + index + '/' + type + '/' + element;

  if (parent !== undefined) {
    putParams = '?routing=' + encodeURIComponent(routing)
      + '&parent=' + encodeURIComponent(parent);
  }

  request.put(putURI + putParams,
              { body: JSON.stringify({}) },
              function(err, resp, body) {
                if(err) return done(err);
                if (resp.statusCode == 400) {
                  return done(new Error("Index failed for '" + putURI + "': " + body));
                }
                if(counter.dec() == 0) {
                  continuation(done);
                }
              });
}

function esCountOrphans(index, type, ptype, callback, done) {
  var uri = baseUrl + index + '/' + type + '/_search?search_type=count';
  var body = {
    filter: {
        not: {
          filter: {
            has_parent: {
              parent_type: ptype,
              query: {
                match_all: {}
              }
            }
          }
        }
    }
  };
  var continuation = function(resp, body, done) {
    if (resp.statusCode == 400) {
      return done(new Error("Query failed for '" + uri + "': " + body));
    }
    if (resp.statusCode == 404) {
      return done(new Error("No such index '" + index + "': " + body));
    }
    callback(JSON.parse(body), done);
  }

  request.post(uri,
               { body: JSON.stringify(body) },
               requestCont(continuation, done));
}
