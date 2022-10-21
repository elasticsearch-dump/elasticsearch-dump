const JSONStream = require('JSONStream')
const fs = require('fs')
const { EOL } = require('os')
const base = require('./base.js')
const StreamSplitter = require('../splitters/streamSplitter.js')
const { PassThrough } = require('stream')
const zlib = require('zlib')
const util = require('util')

class file extends base {
  constructor (parent, file, options) {
    super(parent, file, options)
    this.streamSplitter = new StreamSplitter(parent.options)
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
      lineCounter += this.__handleData(data)
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
