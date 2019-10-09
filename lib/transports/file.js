const util = require('util')
const JSONStream = require('JSONStream')
const fs = require('fs')
const endOfLine = require('os').EOL
const jsonParser = require('../jsonparser.js')
const StreamSplitter = require('../streamSplitter.js')
const { PassThrough } = require('stream')
const zlib = require('zlib')

class file {
  constructor (parent, file, options) {
    this.options = options
    this.parent = parent
    this.file = file
    this.lineCounter = 0
    this.localLineCounter = 0
    this.stream = null
    this.elementsToSkip = 0
    this.streamSplitter = new StreamSplitter(parent.options)
    this.shouldSplit = !!parent.options.fileSize && parent.options.fileSize !== -1
  }

  // accept callback
  // return (error, arr) where arr is an array of objects
  get (limit, offset, callback) {
    this.thisGetLimit = limit
    this.thisGetCallback = callback
    this.localLineCounter = 0

    if (this.lineCounter === 0) {
      this.setupGet(offset)
    } else {
      this.metaStream.resume()
    }

    if (!this.metaStream.readable) {
      this.completeBatch(null, this.thisGetCallback)
    }
  }

  setupGet (offset) {
    this.bufferedData = []
    this.stream = JSONStream.parse()

    if (!this.elementsToSkip) { this.elementsToSkip = offset }

    if (this.file === '$') {
      this.metaStream = process.stdin
    } else {
      this.metaStream = fs.createReadStream(this.file)
    }

    self.stream.on('data', elem => {
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

    self.stream.on('error', e => {
      self.parent.emit('error', e)
    })

    self.stream.on('end', () => {
      self.completeBatch(null, self.thisGetCallback, true)
    })

    let _throughStream = new PassThrough()
    if (this.parent.options.fsCompress) {
      _throughStream = zlib.createGunzip()
    }

    this.metaStream.pipe(_throughStream).pipe(this.stream)
  }

  completeBatch (error, callback, streamEnded) {
    const self = this
    const data = []

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
  set (data, limit, offset, callback) {
    const error = null
    let targetElem

    let lineCounter = 0

    if (!this.stream) {
      if (this.file === '$') {
        this.stream = process.stdout
      } else {
        // TODO: add options to append the file
        if (!this.parent.options.overwrite && fs.existsSync(this.file)) {
          return callback(new Error(`File \`${this.file}\` already exists, quitting`))
        } else if (this.shouldSplit) {
          // do nothing !
        } else {
          let _throughStream = new PassThrough()
          if (this.parent.options.fsCompress) {
            _throughStream = zlib.createGzip()
          }
          _throughStream.pipe(fs.createWriteStream(this.file))
          this.stream = _throughStream
        }
      }
    }

    if (data.length === 0) {
      if (this.file === '$') {
        process.nextTick(() => callback(null, lineCounter))
      } else if (this.shouldSplit) {
        this.streamSplitter.end()
      } else {
        this.stream.on('close', () => {
          delete this.stream
          return callback(null, lineCounter)
        })

        this.stream.end()
      }
    } else {
      data.forEach(elem => {
        // Select _source if sourceOnly
        if (self.parent.options.sourceOnly === true) {
          targetElem = elem._source
        } else {
          targetElem = elem
        }

        if (self.parent.options.format && self.parent.options.format.toLowerCase() === 'human') {
          self.log(util.inspect(targetElem, false, 10, true))
        } else {
          self.log(jsonParser.stringify(targetElem, self.parent))
        }

        lineCounter++
      })

      process.nextTick(() => callback(error, lineCounter))
    }
  }

  log (line) {
    if (this.shouldSplit) {
      this.streamSplitter.write(line)
    } else {
      this.stream.write(line + endOfLine)
    }
  }
}

module.exports = {
  file
}
