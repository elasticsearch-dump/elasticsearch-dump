var fs       = require('fs');
var readline = require('readline');
var stream   = require('stream');

var file = function(parent, file){
  this.file   = file;
  this.parent = parent;
  this.getLineCounter = 0;
  this.setLineCounter = 0;
  this.readLines      = [];
  this.ready          = false;
}

file.prototype.flush = function(limit, callback){
  var self        = this;
  var data        = [];
  self.readStream.pause();
  while(data.length < limit && self.readLines.length > 0 ){
    data.push( self.readLines.pop() );
  }
  callback(null, data);
}

// accept callback
// return (error, arr) where arr is an array of objects
file.prototype.get = function(limit, offset, callback){
  var self        = this;
  
  if(self.readStream == null){
    var instream        = fs.createReadStream(self.file);
    var outstream       = new stream;
    self.readStream     = readline.createInterface(instream, outstream)  
    self.readStream.on('line', function(line){
      try{
        if(line[0] == ','){ line = line.substring(1); }
        self.readLines.push( JSON.parse(line) )
        if(!self.ready || self.readLines.length < limit) return;
        self.ready = false;
        self.flush(limit, callback);
      }catch(e){ }
    });  
    self.readStream.on('close', function(line){
      try{
        if(!self.ready){ self.ready = true; }
        else{ self.flush(limit, callback); }
      }catch(e){ }
    });  
  }

  if(!self.ready && self.readLines.length < limit){
    self.ready = true;
    self.readStream.input.resume();
  }else{
    self.flush(limit, callback);
  }
}

// accept arr, callback where arr is an array of objects
// return (error, writes) 
file.prototype.set = function(data, limit, offset, callback){
  var self         = this;
  var error        = null;

  if(self.writeStream == null){
    // create the file
    try{
      fs.unlinkSync(self.file);
      fs.unlinkSync(self.file);
    }catch(e){ }
    self.writeStream = fs.createWriteStream(self.file, { flags : 'w' });
    self.writeStream.write("[\r\n");
  }

  if(data.length == 0){
    // close the file
    self.writeStream.write("]\r\n");
    setTimeout(function(){
      try{ self.writeStream.close(); }catch(e){}
      callback(error, data.length);
    }, 200)
  }else{   
    var ok = true;
    data.forEach(function(elem){
      if(self.setLineCounter > 0){
        ok = self.writeStream.write(",");
      }
      ok = self.writeStream.write(JSON.stringify(elem) + "\r\n");
      self.setLineCounter++;
    });
    if(ok){
      callback(error, data.length);
    }else{
      self.writeStream.once('drain', function(){
        callback(error, data.length);
      });
    }
  }
  
}

exports.file = file;
