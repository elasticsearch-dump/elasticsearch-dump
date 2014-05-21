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
    this.inputType = 'elasticsearch';
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