var util         = require('util');
var JSONStream   = require('JSONStream');
var endOfLine    = require('os').EOL;

var stdio = function(parent, options){
  this.options = options;
  this.parent = parent;
  this.getStream = JSONStream.parse([true]);
  this.getLineCounter = 0;
  this.setLineCounter = 0;
  this.getData = [];
  this.getCounter = 0;
};

// accept callback
// return (error, arr) where arr is an array of objects
// note that "offset" doesn't make any sense here
stdio.prototype.get = function(limit, offset, callback){
  var self      = this;

  if(self.getCounter === 0){
    self.setup();
  }else{
    process.stdin.resume();
  }

  if(process.stdin.readable && self.getData.length < limit){
    setTimeout(function(){
      self.get(limit, offset, callback);
    }, 1000);
  }else{
    self.completeBatch(null, callback);
  }
};

stdio.prototype.setup = function(){
  var self = this;

  self.getStream.on('data', function(elem){
    self.getCounter++;
    self.getData.push(elem);
  });

  self.getStream.on('error', function(e){
    self.parent.emit('error', e);
  });

  self.getStream.on('end', function(){
    // self.completeBatch(error, callback);
  });

  process.stdin.resume();
  process.stdin.pipe(self.getStream);
};

stdio.prototype.completeBatch = function(error, callback){
  var self = this;
  var data = [];

  process.stdin.pause();

  while(self.getData.length > 0){
    data.push( self.getData.pop() );
  }

  callback(null, data);
};

// accept arr, callback where arr is an array of objects
// return (error, writes) 
stdio.prototype.set = function(data, limit, offset, callback){
  var self = this;
  var error = null;

  // jsonLines means one JSON has per line.  Omit bracket if jsonLines
  if(self.parent.options.jsonLines === false) {
    if(self.setLineCounter === 0){
      log("[");
    }
  }

  data.forEach(function(elem){
    var comma = "";
    // Omit comma if we are outputting jsonLines
    if(self.parent.options.jsonLines === false) {
        if(self.setLineCounter > 0){ comma = ","; }
    }

    // Select _source if sourceOnly
    if(self.parent.options.sourceOnly === true) {
        targetElem = elem["_source"]
    } else {
        targetElem = elem
    }

    if(self.parent.options.format.toLowerCase() === 'human'){
      log(comma + util.inspect(targetElem, false, 10, true));
    }else{
      log(comma + JSON.stringify(targetElem));
    }

    self.setLineCounter++;
  });

  // jsonLines means one JSON has per line.  Omit bracket if jsonLines
  if(self.parent.options.jsonLines === false) {
    if(data.length === 0){
      log("]");
    }
  }

  callback(error, data.length);
};

var log = function(line){
  process.stdout.write(line + endOfLine);
};

exports.stdio = stdio;
