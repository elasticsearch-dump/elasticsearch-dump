var fs = require('fs');

var file = function(parent, options){
  this.options = options;
  this.parent = parent;
}

// accept callback
// return (error, arr) where arr is an array of objects
file.prototype.get = function(limit, offset, callback){
  var self      = this;
  var error     = null; 
  var data      = [];

  callback(err, data); 
}

// accept arr, callback where arr is an array of objects
// return (error, writes) 
file.prototype.set = function(data, limit, offset, callback){
  var self = this;
  var error = null;

  callback(error);
}

exports.file = file;