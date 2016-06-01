var util  = require("util");
var http  = require("http");
var https = require("https");
var EventEmitter = require('events').EventEmitter;
var isUrl = require('./lib/is-url');

var elasticdump = function(input, output, options){
  var self  = this;

  self.input   = input;
  self.output  = output;
  self.options = options;

  if (!self.options.searchBody)  {
      self.options.searchBody = {"query": { "match_all": {} }, "fields": ["*"], "_source": true };
  }

  if (self.options.toLog === null || self.options.toLog === undefined) {
    self.options.toLog = true;
  }

  self.validationErrors = self.validateOptions();

  if(options.maxSockets){
    self.log('globally setting maxSockets=' + options.maxSockets);
    http.globalAgent.maxSockets  = options.maxSockets;
    https.globalAgent.maxSockets = options.maxSockets;
  }

  var InputProto;
  if(self.options.input && !self.options.inputTransport){
    if(isUrl(self.options.input)){
      self.inputType = 'elasticsearch';
    }else{
      self.inputType  = 'file';
    }

    InputProto = require(__dirname + "/lib/transports/" + self.inputType)[self.inputType];
    self.input = (new InputProto(self, self.options.input, self.options['input-index']));
  }else if(self.options.inputTransport){
    self.inputType = String(self.options.inputTransport);
    InputProto = require(self.options.inputTransport);
    var inputProtoKeys = Object.keys(InputProto);
    self.input = (new InputProto[inputProtoKeys[0]](self, self.options.input, self.options['input-index']));
  }

  var OutputProto;
  if(self.options.output && !self.options.outputTransport){
    if(isUrl(self.options.output)){
      self.outputType = 'elasticsearch';
    }else{
      self.outputType = 'file';
      if(self.options.output === "$"){ self.options.toLog = false; }
    }

    OutputProto = require(__dirname + "/lib/transports/" + self.outputType)[self.outputType];
    self.output = (new OutputProto(self, self.options.output, self.options['output-index']));
  }else if(self.options.outputTransport){
    self.outputType = String(self.options.outputTransport);
    OutputProto = require(self.options.outputTransport);
    var outputProtoKeys = Object.keys(OutputProto);
    self.output = (new OutputProto[outputProtoKeys[0]](self, self.options.output, self.options['output-index']));
  }
};

util.inherits(elasticdump, EventEmitter);

elasticdump.prototype.log = function(message){
  var self = this;

  if(typeof self.options.logger === 'function'){
    self.options.logger(message);
  }else if(self.options.toLog === true){
    self.emit("log", message);
  }
};

elasticdump.prototype.validateOptions = function(){
  var self = this;
  var validationErrors = [];

  var required = ['input', 'output'];
  required.forEach(function(v){
    if(!self.options[v]){
      validationErrors.push('`' + v + '` is a required input');
    }
  });

  return validationErrors;
};

elasticdump.prototype.dump = function(callback, continuing, limit, offset, total_writes){
  var self  = this;

  if(self.validationErrors.length > 0){
    self.emit('error', {errors: self.validationErrors});
    callback( new Error('There was an error starting this dump') );
  }else{

    if(!limit){ limit = self.options.limit;  }
    if(!offset){ offset = self.options.offset; }
    if(!total_writes){ total_writes = 0; }

    if(continuing !== true){
      self.log('starting dump');

      if(self.options.offset){
        self.log('Warning: offseting ' + self.options.offset + ' rows.');
        self.log("  * Using an offset doesn't guarantee that the offset rows have already been written, please refer to the HELP text.");
      }
    }

    self.input.get(limit, offset, function(err, data){
      if(err){  self.emit('error', err); }
      if(!err || (self.options['ignore-errors'] === true || self.options['ignore-errors'] === 'true') ){
        self.log("got " + data.length + " objects from source " + self.inputType + " (offset: "+offset+")");
        self.output.set(data, limit, offset, function(err, writes){
          var toContinue = true;

          if(err){
            self.emit('error', err);
            if( self.options['ignore-errors'] === true || self.options['ignore-errors'] === 'true' ){
              toContinue = true;
            }else{
              toContinue = false;
            }
          }else{
            total_writes += writes;
            if(data.length > 0){
              self.log("sent " + data.length + " objects to destination " + self.outputType + ", wrote " + writes);
              offset = offset + data.length;
            }
          }

          if(data.length > 0 && toContinue){
            self.dump(callback, true, limit, offset, total_writes);
          }else if(toContinue){
            self.log('Total Writes: ' + total_writes);
            self.log('dump complete');
            if(typeof callback === 'function'){ callback(null, total_writes); }
          }else if(toContinue === false){
            self.log('Total Writes: ' + total_writes);
            self.log('dump ended with error (set phase)  => ' + String(err));
            if(typeof callback === 'function'){ callback(err, total_writes); }
          }
        });
      }else{
        self.log('Total Writes: ' + total_writes);
        self.log('dump ended with error (get phase) => ' + String(err));
        if(typeof callback === 'function'){ callback(err, total_writes); }
      }
    });
  }
};

exports.elasticdump = elasticdump;
