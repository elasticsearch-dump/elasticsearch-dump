var util         = require('util');
var JSONStream   = require('JSONStream');

var stdio = function(parent, options){
  this.options = options;
  this.parent = parent;
  this.getStream = JSONStream.parse([true]);
  this.getLineCounter = 0;
  this.setLineCounter = 0;
  this.getData = [];
  this.getCounter = 0;
}

// accept callback
// return (error, arr) where arr is an array of objects
// note that "offset" doesn't make any sense here
stdio.prototype.get = function(limit, offset, callback){
  var self      = this;
  var error     = null; 

  if(self.getCounter === 0){
    self.setup();
  }else{
    process.stdin.resume();
  }

  if(process.stdin.readable && self.getData.length < limit){
    setTimeout(function(){
      self.get(limit, offset, callback);
    }, 1000)
  }else{
    self.completeBatch(null, callback);
  }
}

stdio.prototype.setup = function(){
  var self = this;

  self.getStream.on('data', function(elem){
    self.getCounter++;
    // console.log("A")
    self.getData.push(elem);
  });

  self.getStream.on('error', function(e){
    // console.log("B")
    self.parent.emit('error', e);
  });

  self.getStream.on('end', function(){
    // console.log("C")
    // self.completeBatch(error, callback);
  });

  process.stdin.resume();
  process.stdin.pipe(self.getStream);
}

stdio.prototype.completeBatch = function(error, callback){
  var self = this;
  var data = [];

  process.stdin.pause();

  while(self.getData.length > 0){
    data.push( self.getData.pop() );
  }

  callback(null, data);
}

// accept arr, callback where arr is an array of objects
// return (error, writes) 
stdio.prototype.set = function(data, limit, offset, callback){
  var self = this;
  var error = null;

  if(self.setLineCounter === 0){
    console.log("[");
  }

  data.forEach(function(elem){
    var comma = "";
    if(self.setLineCounter > 0){ comma = ","; }
    if(self.parent.options.format.toLowerCase() === 'human'){
      console.log(comma + util.inspect(elem, false, 10, true));
    }else{
      console.log(comma + JSON.stringify(elem));
    }

    self.setLineCounter++;
  });

  if(data.length === 0){
    console.log("]");
  }

  callback(error, data.length);
}

exports.stdio = stdio;