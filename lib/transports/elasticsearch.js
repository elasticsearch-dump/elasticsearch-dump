var request            = require('request');

var elasticsearch = function (parent, baseUrl) {
    this.baseUrl            = baseUrl;
    this.parent             = parent;
    this.lastScrollId       = null;
    this.totalSearchResults = 0;
    this.totalWriting       = 0;
    this.writeConcurency    = 100;
    this.writeSleep         = 0;
};

// accept callback
// return (error, arr) where arr is an array of objects
elasticsearch.prototype.get = function (limit, offset, callback) {
    var searchBody, searchRequest, self, uri;
    self = this;

    if (offset >= self.totalSearchResults && self.totalSearchResults != 0) {
        callback(null, []);
        return;
    }

    if (self.lastScrollId !== null) {
        scrollResultSet(self, callback);
    } else {
        uri = self.baseUrl +
              "/" +
              "_search?search_type=scan&scroll=" +
              self.parent.options.scrollTime +
              "&size=" + self.parent.options.limit;

        searchBody = {
            "query": {
                "match_all": {}
            },
            "size": limit
        };

        searchRequest = {
            "uri": uri,
            "method": "GET",
            "body": JSON.stringify(searchBody)
        };

        request.get(searchRequest, function requestResonse (err, response) {
            if(err != null){
                callback(err, []);
                return;
            }else if(response.statusCode != 200){
                err = new Error(response.body);
                callback(err, []);
                return;
            }

            var body = JSON.parse(response.body);
            self.lastScrollId = body._scroll_id;
            self.totalSearchResults = body.hits.total;

            scrollResultSet(self, callback);
        });
    }
};

// accept arr, callback where arr is an array of objects
// return (error, writes)
elasticsearch.prototype.set = function(data, limit, offset, callback){
    var self = this;
    var error = null;
    var writes = 0;
    if (self.parent.options.bulk === true){
        var thisUrl = self.baseUrl + "/_bulk";
        var payload = {
            url:  thisUrl,
            body: "",
        };
        data.forEach(function(elem, index, theArray) {
            var val = "{ \"index\" : { ";
            val += "\"_index\" : \"" + elem._index + "\", ";
            val += "\"_type\" : \""  + elem._type  + "\", ";
            val += "\"_id\" : \""    + elem._id    + "\"";
            val += " } }\n";
            val += JSON.stringify(elem._source) + "\n";
            theArray[index] = val;
        });
        payload.body = data.join("");
        request.put(payload, function(err, response, body){
            var writes = 0;
            try{
                var r = JSON.parse(response.body);
                if(r.items != null){
                    if(r.ok === true ){
                        writes = data.length;
                    }else{
                        r.items.forEach(function(item){
                            if(item.index.status < 400){
                                writes++;
                            }
                        });
                    }
                }
            }catch(e){
                err = e;
            }
            self.reindex(function(err){
                callback(err, writes)
            });
        });
    }else{
        var started = 0;
        data.forEach(function(elem){
            started++;
            var thisUrl = self.baseUrl + "/";
            if(self.parent.options.all === true){ thisUrl += elem._index + "/"; }
            thisUrl += elem._type + "/" + elem._id;
            var payload = {
                url:  thisUrl,
                body: JSON.stringify(elem._source)
            };
            self.parent.emit('debug', 'thisUrl: ' + thisUrl + ", elem._source: " + JSON.stringify(elem._source));

            write_one(self, payload, function(err, response, body){
                if(err != null){ error = err }
                try{
                    var r = JSON.parse(response.body);
                    if(r.ok == true || r._version >= 1){ writes++; }
                }catch(e){ }
                started--;
                if(started === 0){
                    self.reindex(function(err){
                        callback(error, writes)
                    });
                }
            });
        });

        if(data.length === 0){
            process.nextTick(function(){
                callback(error, writes);
            });
        }
    }
}


elasticsearch.prototype.del = function(elem, callback){
    var self = this;
    var thisUrl = self.baseUrl + "/" + encodeURIComponent(elem._type) + "/" + encodeURIComponent(elem._id);

    self.parent.emit('debug', 'deleteUrl: ' + thisUrl);
    request.del(thisUrl, function(err, response, body){
        if(typeof callback == 'function'){ callback(err, response, body); }
    });
};

elasticsearch.prototype.reindex = function(callback){
    var self = this;
    request.post(self.baseUrl + "/_refresh", function(err, response){
        callback(err, response);
    });
};

exports.elasticsearch = elasticsearch;

/////////////
// HELPERS //
/////////////

function write_one(self, payload, callback){
  if(self.totalWriting < self.writeConcurency){
    self.totalWriting++;
    request.put(payload, function(err, response, body){
      self.totalWriting--;
      callback(err, response, body);
    });
  }else{
    setTimeout(function(){
      write_one(self, payload, callback)
    }, self.writeSleep)
  }
}

/**
 * Posts requests to the _search api to fetch the latest
 * scan result with scroll id
 * @param that
 * @param callback
 */
function scrollResultSet(that, callback) {
    var self = that;
    var baseUrl = self.baseUrl;
    var searchUrl;
    
    if (baseUrl.split('/').length -1 <= 2) {
        searchUrl = baseUrl;
    } else {
        searchUrl = baseUrl.substr(0, baseUrl.lastIndexOf('/'));
    }
    

    scrollRequest = {
        "uri": searchUrl + "/" + "_search" + "/scroll?scroll=" + self.parent.options.scrollTime,
        "method": "POST",
        "body": self.lastScrollId
    };
    request.get(scrollRequest, function requestResonse (err, response) {
        if(response.statusCode != 200 && err == null){
            err = new Error(response.body);
            callback(err, []);
            return;
        }

        var body = JSON.parse(response.body);
        self.lastScrollId = body._scroll_id;
        var hits = body.hits.hits;

        if(self.parent.options.delete === true && hits.length > 0) {
            var stated = 0;
            hits.forEach(function(elem){
                stated++;
                self.del(elem, function(){
                    stated--;
                    if(stated === 0){
                        self.reindex(function(err){
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
            callback(err, hits);
        }
    });
}
