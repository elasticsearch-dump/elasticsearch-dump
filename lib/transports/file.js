var util = require('util')
var JSONStream = require('JSONStream')
var fs = require('fs')
var endOfLine = require('os').EOL

var file = function (parent, file, options) {
  this.options = options
  this.parent = parent
  this.file = file
  this.lineCounter = 0
  this.localLineCounter = 0
  this.stream = null
  this.elementsToSkip = 0
}

// accept callback
// return (error, arr) where arr is an array of objects
file.prototype.get = function (limit, offset, callback) {
  var self = this
  self.thisGetLimit = limit
  self.thisGetCallback = callback
  self.localLineCounter = 0

  if (self.lineCounter === 0) {
    self.setupGet(offset)
  } else {
    self.metaStream.resume()
  }

  if (!self.metaStream.readable) {
    self.completeBatch(null, self.thisGetCallback)
  }
}

file.prototype.setupGet = function (offset) {
  var self = this

  self.bufferedData = []
  self.stream = JSONStream.parse()

  if (!self.elementsToSkip) { self.elementsToSkip = offset }

  if (self.file === '$') {
    self.metaStream = process.stdin
  } else {
    self.metaStream = fs.createReadStream(self.file)
  }

  self.stream.on('data', function (elem) {
    if (self.elementsToSkip > 0) {
      self.elementsToSkip--
    } else {
      self.bufferedData.push(elem)
    }

    self.localLineCounter++
    self.lineCounter++

    if (self.localLineCounter === self.thisGetLimit) {
      self.completeBatch(null, self.thisGetCallback)
    }
  })

  self.stream.on('error', function (e) {
    self.parent.emit('error', e)
  })

  self.stream.on('end', function () {
    self.completeBatch(null, self.thisGetCallback, true)
  })

  self.metaStream.pipe(self.stream)
}

file.prototype.completeBatch = function (error, callback, streamEnded) {
  var self = this
  var data = []

  self.metaStream.pause()

  if (error) { return callback(error) }

  // if we are skipping, have no data, and there is more to read we should continue on
  if (!streamEnded && self.elementsToSkip > 0 && self.bufferedData.length === 0) {
    return self.metaStream.resume()
  }

  while (self.bufferedData.length > 0) {
    data.push(self.bufferedData.pop())
  }

  return callback(null, data)
}

// accept arr, callback where arr is an array of objects
// return (error, writes)
file.prototype.set = function (data, limit, offset, callback) {
  var self = this
  var error = null
  var targetElem

  self.lineCounter = 0

  if (!self.stream) {
    if (self.file === '$') {
      self.stream = process.stdout
    } else {
      // TODO: add options to append and replace the file
      if (fs.existsSync(self.file)) {
        return callback(new Error('File `' + self.file + '` already exists, quitting'))
      } else {
        self.stream = fs.createWriteStream(self.file)
      }
    }
  }

  if (data.length === 0) {
    if (self.file === '$') {
      process.nextTick(callback(null, self.lineCounter))
    } else {
      self.stream.on('close', function () {
        delete self.stream
        return callback(null, self.lineCounter)
      })

      self.stream.end()
    }
  } else {
    data.forEach(function (elem) {
      // Select _source if sourceOnly
      if (self.parent.options.sourceOnly === true) {
        targetElem = elem._source
      } else {
        targetElem = elem
      }

      if (self.parent.options.format && self.parent.options.format.toLowerCase() === 'human') {
        self.log(util.inspect(targetElem, false, 10, true))
      } else {
        self.log(JSON.stringify(targetElem))
      }

      self.lineCounter++
    })

    process.nextTick(function () {
      callback(error, self.lineCounter)
    })
  }
}

file.prototype.log = function (line) {
  this.stream.write(line + endOfLine)
}

exports.file = file
