var fs       = require('fs');
var readline = require('readline');
var stream   = require('stream');

var file = function(parent, file){
  this.file   = file;
  this.parent = parent;
  this.getLineCounter = 0;
  this.setLineCounter = 0;
  this.readLines      = [];
}

// accept callback
// return (error, arr) where arr is an array of objects
file.prototype.get = function(limit, offset, callback){
  var self        = this;
  var data        = [];
  
  if(self.readStream == null){
    var instream        = fs.createReadStream(self.file);
    var outstream       = new stream;
    self.readStream     = readline.createInterface(instream, outstream)  
    self.readStream.on('line', function(line){
      try{
        if(line[0] == ','){ line = line.substring(1); }
        self.readLines.push( JSON.parse(line) )
      }catch(e){ }
    });  
  }

  if(self.readLines.length < limit){
    self.readStream.input.resume();
  }

  setTimeout(function(){
    for(var i in self.readLines){
      while(data.length < limit && self.readLines.length > 0 ){
        data.push( self.readLines.pop() );
      }
    }
    self.readStream.input.pause();
    callback(null, data);
  }, 100)
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
    data.forEach(function(elem){
      if(self.setLineCounter > 0){
        self.writeStream.write(",");
      }
      self.writeStream.write(JSON.stringify(elem) + "\r\n");
      self.setLineCounter++;
    });
    callback(error, data.length);
  }
  
}

exports.file = file;