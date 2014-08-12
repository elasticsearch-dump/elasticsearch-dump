var util  = require("util");
var http  = require("http");
var https = require("https");
var EventEmitter = require('events').EventEmitter;

var elasticdump = function(input, output, options){
  var self  = this;

  self.input   = input;
  self.output  = output;
  self.options = options;
  if (self.options.searchBody == null)  {
      self.options.searchBody = {"query": { "match_all": {} } };
  }

  self.validateOptions();  
  self.toLog = true;

  if(self.options.input == "$"){
    self.inputType = 'stdio'; 
  }else if(self.options.input.indexOf(":") >= 0){
    self.inputType = 'elasticsearch';
  }else{
    self.inputType  = 'file';
  }

  if(self.options.output == "$"){
    self.outputType = 'stdio'; 
    self.toLog = false;
  }else if(self.options.output.indexOf(":") >= 0){
    self.outputType = 'elasticsearch';
  }else{
    self.outputType = 'file';
  }

  if(options.maxSockets != null){
    self.log('globally setting maxSockets=' + options.maxSockets);
    http.globalAgent.maxSockets  = options.maxSockets;
    https.globalAgent.maxSockets = options.maxSockets;
  }

  var inputProto  = require(__dirname + "/lib/transports/" + self.inputType)[self.inputType];
  var outputProto = require(__dirname + "/lib/transports/" + self.outputType)[self.outputType];

  self.input  = (new inputProto(self, self.options.input));
  self.output = (new outputProto(self, self.options.output));
}

util.inherits(elasticdump, EventEmitter);

elasticdump.prototype.log = function(message){
  var self = this;

  if(typeof self.options.logger === 'function'){
    self.options.logger(message);
  }else if(self.toLog === true){
    self.emit("log", message);
  }
}

elasticdump.prototype.validateOptions = function(){
  var self = this;
  // TODO
}

elasticdump.prototype.dump = function(callback, continuing, limit, offset, total_writes){
  var self  = this;
  
  if(limit  == null){ limit = self.options.limit;  }
  if(offset == null){ offset = self.options.offset; }
  if(total_writes == null){ total_writes = 0; }

  if(continuing !== true){
    self.log('starting dump');
  }

  self.input.get(limit, offset, function(err, data){
    if(err){  self.emit('error', err); }
    self.log("got " + data.length + " objects from source " + self.inputType + " (offset: "+offset+")");
    self.output.set(data, limit, offset, function(err, writes){
      var toContinue = true;
      if(err){ 
        self.emit('error', err);
        if( self.options['ignore-errors'] == true || self.options['ignore-errors'] == 'true' ){
          toContinue = true;
        }else{
          toContinue = false;
        }
      }else{
        total_writes += writes;
        self.log("sent " + data.length + " objects to destination " + self.outputType + ", wrote " + writes);
        offset = offset + limit;
      }
      if(data.length > 0 && toContinue){
        self.dump(callback, true, limit, offset, total_writes);
      }else if(toContinue){
        self.log('dump complete');
        if(typeof callback === 'function'){ callback(total_writes); }
      }else if(toContinue == false){
        self.log('dump ended with error');
        if(typeof callback === 'function'){ callback(total_writes); }
      }
    });
  });
}

exports.elasticdump = elasticdump;
