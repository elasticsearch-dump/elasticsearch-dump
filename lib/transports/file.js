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
  // self.readStream = fs.createWriteStream(self.file, { flags : 'r' });

  callback(err, data); 
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