var util = require("util");
var EventEmitter = require('events').EventEmitter;

var elasticdump = function(reader, writer, options){
  this.reader  = reader;
  this.writer  = writer;
  this.options = options;

  this.validateOptions();

  var readerProto = require(__dirname + "/lib/transports/" + this.options.reader.type)[this.options.reader.type];
  var writerProto = require(__dirname + "/lib/transports/" + this.options.writer.type)[this.options.writer.type];

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
    if(err){ self.emit('error', err);
    }else if(data.length > 0){
      self.log("got " + data.length + " objects from source " + self.options.reader.type);
      self.writer.set(data, limit, offset, function(err){
        if(err){ self.emit('error', err);
        }else{
          self.log("set " + data.length + " objects to destination " + self.options.writer.type);
          offset = offset + limit;
        }
        self.dump(callback, true, limit, offset);
      });
    }else{
      self.emit('log', 'dump complete');
      self.emit('done');
      if(typeof callback === 'function'){ callback(); }
    }
  });
}

exports.elasticdump = elasticdump;