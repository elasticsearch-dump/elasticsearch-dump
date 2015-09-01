var fs         = require('fs');
var lineReader = require('line-reader');

var file = function(parent, file){
  this.file   = file;
  this.parent = parent;
  this.getLineCounter = 0;
  this.setLineCounter = 0;
  this.ready          = false;
  this.reader         = null;
};


// accept callback
// return (error, arr) where arr is an array of objects
file.prototype.get = function(limit, offset, callback){
  var self = this;
  
  if(!self.reader){
    lineReader.open(self.file, function(reader){
      self.reader  = reader;
      self.getLines(limit, offset, callback);
    });
  }else{
    self.getLines(limit, offset, callback);
  }
};

file.prototype.getLines = function(limit, offset, callback, data){
  var self = this;

  if(!data){ data = []; }
  
  if(data.length >= limit){
    callback(null, data);
  }else if( self.reader.hasNextLine() ){
    self.reader.nextLine(function(line){
      if(line[0] == ','){ line = line.substring(1); }
      line = line.trim();

      if(line.length > 1){ 
        // are we skipping?
        if(!self.parent.options.skip || (self.parent.options.skip !== null && self.getLineCounter >= self.parent.options.skip)) {
          // no, lets push the data
          data.push( JSON.parse(line) );
        }

        self.getLineCounter++;
      }

      self.getLines(limit, offset, callback, data);
    });
  }else{
    callback(null, data);
  }
};

// accept arr, callback where arr is an array of objects
// return (error, writes) 
file.prototype.set = function(data, limit, offset, callback){
  var self         = this;
  var error        = null;

  if(!self.writeStream){
    // create the file
    try{
      fs.unlinkSync(self.file);
      fs.unlinkSync(self.file);
    }catch(e){ }
    self.writeStream = fs.createWriteStream(self.file, { flags : 'w' });
    self.writeStream.write("[\r\n");
  }

  if(data.length === 0){
    // close the file
    self.writeStream.write("]\r\n");
    setTimeout(function(){
      try{ self.writeStream.end(); }catch(e){}
      callback(error, data.length);
    }, 200);
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
  
};

exports.file = file;
