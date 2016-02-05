var util         = require('util');
var JSONStream   = require('JSONStream');
var fs           = require('fs');
var endOfLine    = require('os').EOL;

var file = function(parent, file, options, direction){
  this.options = options;
  this.parent  = parent;
  this.file    = file;
  this.lineCounter = 0;
  this.localLineCounter = 0;
  this.stream  = null;
};

// accept callback
// return (error, arr) where arr is an array of objects
// note that "offset" doesn't make any sense here
file.prototype.get = function(limit, offset, callback){
  var self = this;
  self.thisGetLimit = limit;
  self.thisGetCallback = callback;
  self.localLineCounter = 0;

  if(self.lineCounter === 0){
    self.setupGet();
  }else{
    self.metaStream.resume();
  }

  if(!self.metaStream.readable){
    self.completeBatch(null, self.thisGetCallback);
  }
};

file.prototype.setupGet = function(){
  var self = this;

  self.bufferedData = [];
  self.stream = JSONStream.parse();

  if(self.file === '$'){
    self.metaStream = process.stdin;
  }else{
    self.metaStream = fs.createReadStream(self.file);
  }

  self.stream.on('data', function(elem){
    if(!self.parent.options.skip || (self.parent.options.skip !== null && self.lineCounter >= self.parent.options.skip)){
      self.bufferedData.push(elem);
    }

    self.localLineCounter++;
    self.lineCounter++;

    if(self.localLineCounter === self.thisGetLimit){
      self.completeBatch(null, self.thisGetCallback);
    }
  });

  self.stream.on('error', function(e){
    self.parent.emit('error', e);
  });

  self.stream.on('end', function(){
    self.completeBatch(null, self.thisGetCallback);
  });

  self.metaStream.pipe(self.stream);
};

file.prototype.completeBatch = function(error, callback){
  var self = this;
  var data = [];

  self.metaStream.pause();

  while(self.bufferedData.length > 0){
    data.push( self.bufferedData.pop() );
  }

  return callback(null, data);
};

// accept arr, callback where arr is an array of objects
// return (error, writes)
file.prototype.set = function(data, limit, offset, callback){
  var self = this;
  var error = null;

  self.lineCounter = 0;

  if(!self.stream){
    if(self.file === '$'){
      self.stream = process.stdout;
    }else{
      // TODO: add options to append and replace the file
      if(fs.existsSync(self.file)){
        return callback(new Error('File `' + self.file + '` already exists, quitting'));
      }else{
        self.stream = fs.createWriteStream(self.file);
      }
    }
  }

  if(data.length === 0){
    if(self.file === '$'){
      process.nextTick(callback(null, self.lineCounter));
    }else{
      self.stream.on('close', function(){
        delete self.stream;
        return callback(null, self.lineCounter);
      });

      self.stream.end();
    }
  }else{

    data.forEach(function(elem){
      // Select _source if sourceOnly
      if(self.parent.options.sourceOnly === true) {
        targetElem = elem._source;
      }else{
        targetElem = elem;
      }

      if(self.parent.options.format && self.parent.options.format.toLowerCase() === 'human'){
        self.log(util.inspect(targetElem, false, 10, true));
      }else{
        self.log(JSON.stringify(targetElem));
      }

      self.lineCounter++;
    });

    process.nextTick(function(){
      callback(error, self.lineCounter);
    });
  }
};

file.prototype.log = function(line){
  this.stream.write(line + endOfLine);
};

exports.file = file;
