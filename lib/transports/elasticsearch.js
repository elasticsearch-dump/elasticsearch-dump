var request = require('request');
var jsonParser = require('../jsonparser.js');
var parseBaseURL = require('../parse-base-url');

var elasticsearch = function(parent, url, index) {
  this.base = parseBaseURL(url, index);
  this.parent = parent;
  this.lastScrollId = null;
  this.totalSearchResults = 0;
  this.hasSkipped = 0;
};
// accept callback
// return (error, arr) where arr is an array of objects
elasticsearch.prototype.get = function(limit, offset, callback) {
  var self = this;
  var type = self.parent.options.type;
  if (type === 'data') {
    self.getData(limit, offset, callback);
  } else if (type === 'mapping') {
    self.getMapping(limit, offset, callback);
  } else if (type === 'analyzer') {
    self.getSettings(limit, offset, callback);
  } else {
    callback(new Error('unknown type option'), null);
  }
};

elasticsearch.prototype.getMapping = function(limit, offset, callback) {
  var self = this;
  if (self.gotMapping === true) {
    callback(null, []);
  } else {
    var url = self.base.url + '/_mapping';
    request.get(url, function(err, response) {
      self.gotMapping = true;
      var payload = [];
      if (!err) {
        response = payload.push(JSON.parse(response.body));
      }
      callback(err, payload);
    });
  }
};

elasticsearch.prototype.getSettings = function(limit, offset, callback) {
  var self = this;
  if (self.gotSettings === true) {
    callback(null, []);
  } else {
    var url = self.base.url + '/_settings';
    request.get(url, function(err, response) {
      self.gotSettings = true;
      var payload = [];
      if (!err) {
        response = payload.push(response.body);
      }
      callback(err, payload);
    });
  }
};

elasticsearch.prototype.getData = function(limit, offset, callback) {
  var searchRequest, self, uri;
  self = this;
  var searchBody = self.parent.options.searchBody;

  if (offset >= self.totalSearchResults && self.totalSearchResults !== 0) {
    callback(null, []);
    return;
  }

  if (self.lastScrollId !== null) {
    scrollResultSet(self, callback);
  } else {
    self.numberOfShards(self.base, function(err, numberOfShards) {
      var shardedLimit = Math.ceil(limit / numberOfShards);

      // previously we used the scan/scroll method, but now we need to change the sort
      // https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking_50_search_changes.html#_literal_search_type_scan_literal_removed

      uri = self.base.url +
        "/" +
        "_search?scroll=" +
        self.parent.options.scrollTime +
        "&size=" + shardedLimit;

      searchBody.size = shardedLimit;

      searchRequest = {
        "uri": uri,
        "method": "GET",
        "sort": [ "_doc" ],
        "body": JSON.stringify(searchBody)
      };

      request.get(searchRequest, function requestResonse(err, response) {
        if (err) {
          callback(err, []);
          return;
        } else if (response.statusCode !== 200) {
          err = new Error(response.body);
          callback(err, []);
          return;
        }

        var body = jsonParser.parse(response.body);
        self.lastScrollId = body._scroll_id;

        if (self.lastScrollId === undefined) {
          err = new Error("Unable to obtain scrollId; This tends to indicate an error with your index(es)");
          callback(err, []);
          return;
        }
        self.totalSearchResults = body.hits.total;

        scrollResultSet(self, callback, body.hits.hits);
      });
    });
  }
};

// to respect the --limit param, we need to set the scan/sroll limit = limit/#Shards
// http://www.elasticsearch.org/guide/en/elasticsearch/guide/current/scan-scroll.html
elasticsearch.prototype.numberOfShards = function(base, callback) {
  request.get(base.url + "/_settings", function(err, response) {
    if (err) {
      callback(err);
    } else {
      try {
        var body = jsonParser.parse(response.body);
        var numberOfShards = body[base.index].settings.index.number_of_shards;
        callback(err, numberOfShards);
      } catch (e) {
        callback(err, 1);
      }
    }
  });
};

// accept arr, callback where arr is an array of objects
// return (error, writes)
elasticsearch.prototype.set = function(data, limit, offset, callback) {
  var self = this;
  var type = self.parent.options.type;
  if (type === 'data') {
    self.setData(data, limit, offset, callback);
  } else if (type === 'mapping') {
    self.setMapping(data, limit, offset, callback);
  } else if (type === 'analyzer') {
    self.setAnalyzer(data, limit, offset, callback);
  } else {
    callback(new Error('unknown type option'), null);
  }
};

elasticsearch.prototype.setMapping = function(data, limit, offset, callback) {
  var self = this;
  if (self.haveSetMapping === true) {
    callback(null, 0);
  } else {
    request.put(self.base.url, function(err, response) { // ensure the index exists
      try {
        data = data[0];
      } catch (e) {
        return callback(e);
      }
      var started = 0;
      var count = 0;
      for (var index in data) {
        var mappings = data[index]['mappings'];
        var sortedMappings = [];

        // TODO: it seems we need to set the mappings for children with parents first?
        for(var key in mappings){
          if(mappings[key]._parent){
            sortedMappings = [{key: key, data: mappings[key]}].concat(sortedMappings);
          }else{
            sortedMappings.push({key: key, data: mappings[key]});
          }
        }

        sortedMappings.forEach(function(set){
          var mapping = {};
          var url = self.base.url + '/' + encodeURIComponent(set.key) + '/_mapping';
          started++;
          count++;

          var payload = {
            url: url,
            body: JSON.stringify(set.data),
            timeout: self.parent.options.timeout,
          };

          request.put(payload, function(err, response) { // upload the mapping
            started--;
            if (!err) {
              var bodyError = jsonParser.parse(response.body).error;
              if (bodyError) {
                err = bodyError;
              }
            }
            if (started === 0) {
              self.haveSetMapping = true;
              callback(err, count);
            }
          });
        });
      }
    });
  }
};

elasticsearch.prototype.setAnalyzer = function(data, limit, offset, callback) {
  var self = this;
  var updateAnalyzer = function(err, response) {
    try {
      data = jsonParser.parse(data[0]);
    } catch (e) {
      return callback(e);
    }
    var started = 0;
    var count = 0;
    for (var index in data) {
      var settings = data[index]['settings'];
      for (var key in settings) { // iterate through settings
        var setting = {};
        setting[key] = settings[key];
        var url = self.base.url + '/_settings';
        started++;
        count++;

        // ignore all other settings other than 'analysis'
        for (var p in setting[key]) { // iterate through index
          if (p != 'analysis') { // remove everything not 'analysis'
            delete setting[key][p]
          }
        }

        var closeUrl = self.base.url + '/_close'; // close the index
        request.post({
          url: closeUrl,
          timeout: self.parent.options.timeout
        }, function(err, response, body) {
          if (!err) {
            var bodyError = jsonParser.parse(response.body).error;
            if (bodyError) {
              err = bodyError;
            }
            var payload = {
              url: url,
              body: JSON.stringify(setting),
              timeout: self.parent.options.timeout
            };
            request.put(payload, function(err, response) { // upload the analysis settings
              started--;
              if (!err) {
                var bodyError = jsonParser.parse(response.body).error;
                if (bodyError) {
                  err = bodyError;
                }
              } else {
                callback(err, count);
              }
              if (started === 0) {
                self.haveSetAnalyzer = true;
                var openUrl = self.base.url + '/_open'; // open the index
                request.post({
                  url: openUrl,
                  timeout: self.parent.options.timeout
                }, function(err, response) {
                  if (!err) {
                    var bodyError = jsonParser.parse(response.body).error;
                    if (bodyError) {
                      err = bodyError;
                    }
                  }
                  callback(err, count);
                });
              }
            });
          } else {
            callback(err, count);
          }
        });
      }
    }
  }
  if (self.haveSetAnalyzer === true) {
    callback(null, 0);
  } else {
    request.put(self.base.url, function(err, response) { // ensure the index exists
      // use cluster health api to check if the index is ready
      request.get(self.base.host + '/_cluster/health/' + self.base.index + '?wait_for_status=green', updateAnalyzer);
    });
  }
};

elasticsearch.prototype.setData = function(data, limit, offset, callback) {
  var self = this;
  var error = null;
  var extraFields = ['routing', 'parent', 'timestamp', 'ttl'];
  var writes = 0;
  if (data.length === 0) {
    callback(error, writes);
    return;
  }

  var started = 0;
  data.forEach(function(elem) {
    started++;
    var thisUrl = self.base.url + "/";
    if (self.parent.options.all === true) {
      thisUrl += elem._index + "/";
    }
    thisUrl += encodeURIComponent(elem._type) + "/" + encodeURIComponent(elem._id);

    var and = "?";
    extraFields.forEach(function(field) {
      if(elem.fields){
        if (elem.fields[field]) {
          thisUrl += and + field + "=" + encodeURIComponent(elem.fields[field]);
          and = "&";
        }
        if (elem.fields['_' + field]) {
          thisUrl += and + field + "=" + encodeURIComponent(elem.fields['_' + field]);
          and = "&";
        }
      }else{
        if (elem[field]) {
          thisUrl += and + field + "=" + encodeURIComponent(elem[field]);
          and = "&";
        }
        if (elem['_' + field]) {
          thisUrl += and + field + "=" + encodeURIComponent(elem['_' + field]);
          and = "&";
        }
      }
    });

    var payload = {
      url: thisUrl,
      body: JSON.stringify(elem._source),
      timeout: self.parent.options.timeout,
    };
    self.parent.emit('debug', 'thisUrl: ' + thisUrl + ", elem._source: " + JSON.stringify(elem._source));

    request.put(payload, function(err, response) {
      if (err) {
        error = err;
      }
      try {
        var r = jsonParser.parse(response.body);
        if (r.ok === true || r._version >= 1) {
          writes++;
        }
      } catch (e) {}
      started--;
      if (started === 0) {
        self.reindex(function() {
          callback(error, writes);
        });
      }
    });
  });

  if (data.length === 0) {
    process.nextTick(function() {
      callback(error, writes);
    });
  }
};


elasticsearch.prototype.del = function(elem, callback) {
  var self = this;
  var thisUrl = self.base.url + "/" + encodeURIComponent(elem._type) + "/" + encodeURIComponent(elem._id);

  self.parent.emit('debug', 'deleteUrl: ' + thisUrl);
  request.del(thisUrl, function(err, response, body) {
    if (typeof callback === 'function') {
      callback(err, response, body);
    }
  });
};

elasticsearch.prototype.reindex = function(callback) {
  var self = this;
  request.post(self.base.url + "/_refresh", function(err, response) {
    callback(err, response);
  });
};

exports.elasticsearch = elasticsearch;

/////////////
// HELPERS //
/////////////

/**
 * Posts requests to the _search api to fetch the latest
 * scan result with scroll id
 * @param that
 * @param callback
 */
function scrollResultSet(that, callback, loadedHits){
  var self = that;
  var body;

  if(loadedHits && loadedHits.length > 0){

    if (self.parent.options.delete === true) {
      var started = 0;
      loadedHits.forEach(function(elem) {
        started++;
        self.del(elem, function() {
          started--;
          if (started === 0) {
            self.reindex(function(err) {
              callback(err, loadedHits);
            });
          }
        });
      });
    }else{
      callback(null, loadedHits);
    }

  }else{

    var scrollRequest = {
      "uri": self.base.host + "/_search" + "/scroll?scroll=" + self.parent.options.scrollTime,
      "method": "POST",
      "body": self.lastScrollId
    };

    request.get(scrollRequest, function requestResonse(err, response) {
      if (err) {
        callback(err, []);
        return;
      }

      if (err === null && response.statusCode != 200) {
        err = new Error(response.body);
        callback(err, []);
        return;
      }

      try {
        body = jsonParser.parse(response.body);
      } catch (e) {
        e.message = e.message + " | Cannot Parse: " + response.body;
        callback(e, []);
        return;
      }

      self.lastScrollId = body._scroll_id;
      var hits = body.hits.hits;

      if (self.parent.options.delete === true && hits.length > 0) {
        var started = 0;
        hits.forEach(function(elem) {
          started++;
          self.del(elem, function() {
            started--;
            if (started === 0) {
              self.reindex(function(err) {
                if (hits.length === 0) {
                  self.lastScrollId = null;
                }
                callback(err, hits);
              });
            }
          });
        });
      } else {
        if (hits.length === 0) {
          self.lastScrollId = null;
        }

        // are we skipping and we have hits?
        if (self.parent.options.skip !== null && hits.length > 0 && self.hasSkipped < self.parent.options.skip) {
          // lets remove hits until we reach the skip number
          while (hits.length > 0 && self.hasSkipped < self.parent.options.skip) {
            self.hasSkipped++;
            hits.splice(0, 1);
          }

          if (hits.length > 0) {
            // we have some hits after skipping, lets callback
            callback(err, hits);
          } else {
            // we skipped, but now we don't have any hits,
            // scroll again for more data if possible
            scrollResultSet(that, callback);
          }
        } else {
          // not skipping or done skipping
          callback(err, hits);
        }
      }
    });
  }
}
