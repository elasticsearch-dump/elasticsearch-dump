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
  var payload   = {
    size: limit,
    from: offset,
    // TODO: query
  }

  self.parent.emit('debug', 'searchUrl: ' + searchUrl + ", payload: " + JSON.stringify(payload));

  request.get(paramiterizeURL(searchUrl, payload), function(err, response, body){
    try{
      data = (JSON.parse(response.body))['hits']['hits'];
    }catch(e){}
    callback(err, data);
  });  
}

// accept arr, callback where arr is an array of objects
// return (error, writes) 
elasticsearch.prototype.set = function(data, limit, offset, callback){
  var self = this;
  var error = null;
  var stated = 0;
  var writes = 0;
  data.forEach(function(elem){
    stated++;
    var thisUrl = self.baseUrl + "/" + elem._type + "/" + elem._id;
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
        callback(error, writes);
      }
    });
  });
}

var paramiterizeURL = function(url, payload){
  url = url + "?";
  for(var i in payload){
    url = url + "&" + i + "=" + payload[i];
  }
  return url;
}

exports.elasticsearch = elasticsearch;