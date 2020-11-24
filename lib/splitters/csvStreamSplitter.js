/**
 * Created by ferron on 10/3/18 1:04 PM
 * adapted from split-file-stream library
 * URL : https://github.com/dannycho7/split-file-stream/blob/master/index.js
 * credit to : @dannycho7
 */

const fs = require('fs')
const StreamSplitter = require('./streamSplitter')
const { PassThrough } = require('stream')
const zlib = require('zlib')
const jsonParser = require('../jsonparser.js')

class csvStreamSplitter extends StreamSplitter {
  constructor (file, options, context) {
    super(options)
    this._ctx = context
    this.rootFilePath = file
  }

  _outStreamCreate (partitionNum) {
    const _csvStream = this._ctx.createCsvStream()

    let _throughStream = new PassThrough()
    if (this.compress) {
      _throughStream = zlib.createGzip()
    }
    _csvStream.pipe(_throughStream).pipe(fs.createWriteStream(StreamSplitter.generateFilePath(this.rootFilePath, partitionNum, this.compress)))
    return _csvStream
  }

  sizeOf (chunk) {
    const s = jsonParser.stringify(chunk)
    return s && s.length > 0 ? s.length : 0
  }

  write (chunk) {
    if (!this.openStream) {
      this.currentOutStream = this._outStreamCreate(this.partitionNum)
      this.partitionNum++
      this.openStream = true
    }

    this.currentOutStream.write(chunk)

    if (this.splitBySize) {
      // poor man's sizeOf
      this.currentFileSize += this.sizeOf(chunk)
    } else {
      this.currentFileSize++
    }

    if (this.currentFileSize >= this.partitionStreamSize || !chunk) {
      this._endCurrentWriteStream()
    }
  }
}

module.exports = csvStreamSplitter
