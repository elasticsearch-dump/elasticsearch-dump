const JSONStream = require('JSONStream')
const fs = require('fs')
const { EOL } = require('os')
const base = require('./base.js')
const StreamSplitter = require('../splitters/streamSplitter.js')
const { finished, PassThrough } = require('stream')
const zlib = require('zlib')
const util = require('util')

class file extends base {
  constructor (parent, file, options) {
    super(parent, file, options)
    this.streamSplitter = new StreamSplitter(parent.options, this)
    this.pendingWrites = 0
    this.closeCallback = null
    this.transportType = 'file'
  }

  async setupGet (offset) {
    this.bufferedData = []
    this.stream = JSONStream.parse()

    if (!this.elementsToSkip) { this.elementsToSkip = offset }

    if (this.file === '$') {
      this.metaStream = process.stdin
    } else {
      this.metaStream = fs.createReadStream(this.file)
    }

    this.__setupStreamEvents()

    this._throughStream = new PassThrough()
    if (this.parent.options.fsCompress) {
      this._throughStream = zlib.createGunzip()
    }

    try {
      this.metaStream.pipe(this._throughStream).pipe(this.stream)
    } catch (err) {
      throw new Error(util.inspect({ msg: 'Pipe failed', err }))
    }
  }

  // accept arr, callback where arr is an array of objects
  // return (error, writes)
  set (data, limit, offset, callback) {
    const error = null
    let lineCounter = 0

    if (!this.stream) {
      if (this.file === '$') {
        this.stream = process.stdout
      } else {
        if (!this.parent.options.overwrite && fs.existsSync(this.file)) {
          return callback(new Error(`File \`${this.file}\` already exists, quitting`))
        } else if (!this.shouldSplit) {
          let _throughStream = new PassThrough()
          if (this.parent.options.fsCompress) {
            _throughStream = zlib.createGzip({ level: this.parent.options.compressionLevel })
          }
          const fileStream = fs.createWriteStream(this.file)
          _throughStream.pipe(fileStream)
          this.stream = _throughStream

          // Track the underlying file stream
          this.fileStream = fileStream
        }
      }
    }

    if (data.length === 0) {
      if (this.file === '$') {
        process.nextTick(() => callback(null, lineCounter))
      } else {
        if (this.pendingWrites > 0) {
          this.closeCallback = () => this.finalizeStream(callback, lineCounter)
        } else {
          this.finalizeStream(callback, lineCounter)
        }
      }
    } else {
      this.pendingWrites++
      lineCounter += this.__handleData(data)

      // Use nextTick to ensure write operations are queued
      process.nextTick(() => {
        this.pendingWrites--
        if (this.pendingWrites === 0 && this.closeCallback) {
          this.closeCallback()
        } else {
          callback(error, lineCounter)
        }
      })
    }
  }

  finalizeStream (callback, lineCounter) {
    if (this.shouldSplit) {
      this.streamSplitter.ensureFinished((err) => {
        this.closeCallback = null
        callback(err, lineCounter)
      })
      this.streamSplitter.end()
    } else {
      if (!this.stream) {
        return callback(null, lineCounter)
      }

      // We just need to wait for the end of the pipeline to close
      // as the end() propagates through the pipeline
      const streamToWait = this.fileStream || this.stream
      finished(streamToWait, (err) => {
        delete this.stream
        delete this.fileStream
        this.closeCallback = null
        callback(err, lineCounter)
      })

      // Trigger close of whole pipeline
      this.stream.end()
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
