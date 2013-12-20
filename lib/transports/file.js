var fs = require('fs');

var file = function(parent, file){
  this.file   = file;
  this.parent = parent;
  this.getLineCounter = 0;
  this.setLineCounter = 0;
}

// accept callback
// return (error, arr) where arr is an array of objects
file.prototype.get = function(limit, offset, callback){
  var self        = this;
  var error       = null; 
  var data        = [];
  var counter     = 0;

  if(self.readStream == null){
    self.readStream = fs.createReadStream(self.file, { flags : 'r' });
    self.readStream.setEncoding('utf8');
    self.readStream.pause();
  }

  self.readStream.addListener('data', function(lines){
    self.readStream.pause();
    lines = lines.split("\r\n");
    lines.forEach(function(line){
      try{
        if(line[0] === ','){ line = line.substring(1); }
        data.push( JSON.parse(line) )
      }catch(e){
        //
      }
      if(data.length === limit){
        callback(error, data);
      }else{
        self.readStream.resume();
      }
    });
  });

  self.readStream.addListener('end', function(){
    callback(error, data);
  });

  self.readStream.addListener('error', function(err){
    throw(err)
  });

  self.readStream.resume();
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
      self.writeStream.close();
    }, 1000)
  }
  
  data.forEach(function(elem){
    if(self.setLineCounter > 0){
      self.writeStream.write(",");
    }
    self.writeStream.write(JSON.stringify(elem) + "\r\n");
    self.setLineCounter++;
  });

  callback(error, data.length);
}

exports.file = file;