var util = require("util");
var EventEmitter = require('events').EventEmitter;

var elasticdump = function(input, output, options){
  this.input   = input;
  this.output  = output;
  this.options = options;

  this.validateOptions();  
  this.toLog = true;

  if(this.options.input == "$"){
    this.inputType = 'stdio'; 
  }else if(this.options.input.indexOf(":") >= 0){
    if(this.options.scan) {
      this.inputType = 'elasticsearchScan';
    } else {
      this.inputType = 'elasticsearch';
    }
  }else{
    this.inputType  = 'file';
  }

  if(this.options.output == "$"){
    this.outputType = 'stdio'; 
    this.toLog = false;
  }else if(this.options.output.indexOf(":") >= 0){
    this.outputType = 'elasticsearch';
  }else{
    this.outputType = 'file';
  }

  var inputProto  = require(__dirname + "/lib/transports/" + this.inputType)[this.inputType];
  var outputProto = require(__dirname + "/lib/transports/" + this.outputType)[this.outputType];

  this.input  = (new inputProto(this, this.options.input));
  this.output = (new outputProto(this, this.options.output));
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

elasticdump.prototype.dump = function(callback, continuing, limit, offset){
  var self  = this;
  if(limit  == null){ limit = self.options.limit   }
  if(offset == null){ offset = self.options.offset }

  if(continuing !== true){
    self.log('starting dump');
  }

  self.input.get(limit, offset, function(err, data){
    if(err){  self.emit('error', err); }
    self.log("got " + data.length + " objects from source " + self.inputType + " (offset: "+offset+")");
    self.output.set(data, limit, offset, function(err, writes){
      if(err){ self.emit('error', err);
      }else{
        self.log("sent " + data.length + " objects to destination " + self.outputType + ", wrote " + writes);
        offset = offset + limit;
      }
      if(data.length > 0){
        self.dump(callback, true, limit, offset);
      }else{
        self.log('dump complete');
        if(typeof callback === 'function'){ callback(); }
      }
    });
  });
}

exports.elasticdump = elasticdump;