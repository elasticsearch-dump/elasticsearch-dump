var util = require("util");
var EventEmitter = require('events').EventEmitter;

var elasticdump = function(reader, writer, options){
  this.reader  = reader;
  this.writer  = writer;
  this.options = options;

  this.validateOptions();

  this.readerType = 'file';
  this.writerType = 'file';

  if(this.options.reader.indexOf(":") >= 0){ this.readerType = 'elasticsearch'; }
  if(this.options.writer.indexOf(":") >= 0){ this.writerType = 'elasticsearch'; }

  var readerProto = require(__dirname + "/lib/transports/" + this.readerType)[this.readerType];
  var writerProto = require(__dirname + "/lib/transports/" + this.writerType)[this.writerType];

  this.reader = (new readerProto(this, this.options.reader));
  this.writer = (new writerProto(this, this.options.writer));
}

util.inherits(elasticdump, EventEmitter);

elasticdump.prototype.log = function(message){
  var self = this;
  if(typeof self.options.logger === 'function'){
    self.options.logger(message);
  }else{
    self.emit("log", message);
  }
}

elasticdump.prototype.validateOptions = function(){
  var self = this;
  // TODO
}

elasticdump.prototype.dump = function(callback, continuing, limit, offset){
  var self = this;
  if(limit == null){  limit = self.options.limit   }
  if(offset == null){ offset = self.options.offset }

  if(continuing !== true){
    self.emit('log', 'starting dump');
  }

  self.reader.get(limit, offset, function(err, data){
    if(err){  self.emit('error', err); }
    self.log("got " + data.length + " objects from source " + self.readerType + " (offset: "+offset+")");
    self.writer.set(data, limit, offset, function(err, writes){
      if(err){ self.emit('error', err);
      }else{
        self.log("sent " + data.length + " objects to destination " + self.writerType + ", wrote " + writes);
        offset = offset + limit;
      }
      if(data.length > 0){
        self.dump(callback, true, limit, offset);
      }else{
        self.emit('log', 'dump complete');
        self.emit('done');
        if(typeof callback === 'function'){ callback(); }
      }
    });
  });
}

exports.elasticdump = elasticdump;