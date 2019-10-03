const JSONStream = require('JSONStream')
const fs = require('fs')
const { EOL } = require('os')
const base = require('./base.js')
const StreamSplitter = require('../streamSplitter.js')

class file extends base {
  constructor (parent, file, options) {
    super(parent, file, options)
    this.streamSplitter = new StreamSplitter(parent.options)
    this.shouldSplit = !!parent.options.fileSize && parent.options.fileSize !== -1
  }

  setupGet (offset) {
    const self = this

    self.bufferedData = []
    self.stream = JSONStream.parse()

    if (!self.elementsToSkip) { self.elementsToSkip = offset }

    if (self.file === '$') {
      self.metaStream = process.stdin
    } else {
      self.metaStream = fs.createReadStream(self.file)
    }

    this.__setupStreamEvents()

    self.metaStream.pipe(self.stream)
  }

  // accept arr, callback where arr is an array of objects
  // return (error, writes)
  set (data, limit, offset, callback) {
    const self = this
    const error = null
    const lineCounter = 0

    if (!self.stream) {
      if (self.file === '$') {
        self.stream = process.stdout
      } else {
        // TODO: add options to append the file
        if (!self.parent.options.overwrite && fs.existsSync(self.file)) {
          return callback(new Error('File `' + self.file + '` already exists, quitting'))
        } else if (self.shouldSplit) {
          // do nothing !
        } else {
          self.stream = fs.createWriteStream(self.file)
        }
      }
    }

    if (data.length === 0) {
      if (self.file === '$') {
        process.nextTick(() => callback(null, lineCounter))
      } else if (self.shouldSplit) {
        self.streamSplitter.end()
      } else {
        self.stream.on('close', () => {
          delete self.stream
          return callback(null, lineCounter)
        })

        self.stream.end()
      }
    } else {
      this.__handleData(data, lineCounter)

      process.nextTick(() => callback(error, lineCounter))
    }
  }

  log (line) {
    if (this.shouldSplit) {
      this.streamSplitter.write(line)
    } else {
      this.stream.write(line + EOL)
    }
  }
}

module.exports = {
  file
}
