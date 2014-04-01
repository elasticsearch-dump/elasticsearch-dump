var request = require('request');

var elasticsearch = function(parent, baseUrl){
  this.baseUrl = baseUrl;
  this.parent = parent;
}

// accept callback
// return (error, arr) where arr is an array of objects
elasticsearch.prototype.get = function(limit, offset, callback){
  var self      = this;
  var error     = null; 
  var data      = [];
  var searchUrl = self.baseUrl + "/" + "_search";

  if(self.parent.options.delete === true){
    // as we are deleting as we go, the offset should always start at 0
    offset = 0;
  }

  var payload   = {
    size: limit,
    from: offset,
    // TODO: query
  }

  self.parent.emit('debug', 'searchUrl: ' + searchUrl + ", payload: " + JSON.stringify(payload));

  request.get(paramiterizeURL(searchUrl, payload), function(err, response, body){
    if(response.statusCode != 200 && err == null){
      err = new Error(response.body);
    }
    try{
      data = (JSON.parse(response.body))['hits']['hits'];
      if(self.parent.options.delete === true && data.length > 0){
        var stated = 0;
        data.forEach(function(elem){
          stated++;
          self.del(elem, function(){
            stated--;
            if(stated === 0){ 
              self.reindex(function(err){
                callback(err, data); 
              });
            }
          });
        });
      }else{
        callback(err, data);
      }
    }catch(e){
      callback(err, data);
    }    
  });  
}

elasticsearch.prototype.del = function(elem, callback){
  var self = this;
  var thisUrl = self.baseUrl + "/" + encodeURIComponent(elem._type) + "/" + encodeURIComponent(elem._id);

  self.parent.emit('debug', 'deleteUrl: ' + thisUrl);

  request.del(thisUrl, function(err, response, body){
    if(typeof callback == 'function'){ callback(err, response, body); }
  });
}

elasticsearch.prototype.reindex = function(callback){
  var self = this;
  request.post(self.baseUrl + "/_refresh", function(err, response){
    callback(err, response);
  });
}

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
    var stated = 0;
    data.forEach(function(elem){
      stated++;
      var thisUrl = self.baseUrl + "/";
      if(self.parent.options.all === true){ thisUrl += elem._index + "/"; }
      thisUrl += elem._type + "/" + elem._id;
      var payload = {
        url:  thisUrl,
        body: JSON.stringify(elem._source),
      };
      self.parent.emit('debug', 'thisUrl: ' + thisUrl + ", elem._source: " + JSON.stringify(elem._source));
      request.put(payload, function(err, response, body){
        try{
          var r = JSON.parse(response.body);
          if(r.ok == true){ writes++; }
        }catch(e){ }
        stated--;
        if(stated === 0){
          self.reindex(function(err){
            callback(error, writes)
          });
        }
      });
    });
    if(data.length === 0){
      callback(error, writes);
    }
  }
}

var paramiterizeURL = function(url, payload){
  url = url + "?";
  for(var i in payload){
    url = url + "&" + i + "=" + payload[i];
  }
  return url;
}

exports.elasticsearch = elasticsearch;
