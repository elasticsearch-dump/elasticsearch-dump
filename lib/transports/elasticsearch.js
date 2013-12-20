var request = require('request');

var elasticsearch = function(parent, options){
  this.options = options;
  this.parent = parent;
}

// accept callback
// return (error, arr) where arr is an array of objects
elasticsearch.prototype.get = function(limit, offset, callback){
  var self      = this;
  var error     = null; 
  var data      = [];
  var baseUrl   = self.options.transport + "://" + self.options.host + ":" + self.options.port;
  var searchUrl = baseUrl + "/" + self.options.index + "/" + "_search";
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
// return (error) 
elasticsearch.prototype.set = function(data, limit, offset, callback){
  var self = this;
  var error = null;
  var baseUrl   = self.options.transport + "://" + self.options.host + ":" + self.options.port;
  var putUrl = baseUrl + "/" + self.options.index;

  var stated = 0;
  data.forEach(function(elem){
    stated++;
    var thisUrl = putUrl + "/" + elem._type + "/" + elem._id;
    // var payload = {form: {body: JSON.stringify(elem._source)}};
    var payload = {
      url:  thisUrl,
      body: JSON.stringify(elem._source),
    };
    self.parent.emit('debug', 'thisUrl: ' + thisUrl + ", elem._source: " + JSON.stringify(elem._source));
    request.put(payload, function(err, response, body){
      console.log(err)
      console.log(response.body)
      stated--;
      if(stated === 0){
        callback(error);
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