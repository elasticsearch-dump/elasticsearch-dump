/**
 * Created by ferron on 10/3/18 1:04 PM
 * adapted from split-file-stream library
 * URL : https://github.com/dannycho7/split-file-stream/blob/master/index.js
 * credit to : @dannycho7
 */

const fs = require('fs')
const bytes = require('bytes')
const endOfLine = require('os').EOL
const { EventEmitter } = require('events')

class StreamSplitter extends EventEmitter {
  constructor (options) {
    super()
    this.partitionStreamSize = bytes(options.fileSize)
    this.rootFilePath = options.output
    this.outStreams = []
    this.partitionNames = []
    this.currentOutStream = null
    this.currentFileSize = 0
    this.finishedWriteStreams = 0
    this.openStream = false
    this.partitionNum = 0
  }

  static generateFilePath (rootFileName, numFiles) {
    return `${rootFileName}.split-${numFiles}`
  }

  _outStreamCreate (partitionNum) {
    return fs.createWriteStream(StreamSplitter.generateFilePath(this.rootFilePath, partitionNum))
  }

  _endCurrentWriteStream () {
    this.currentOutStream.end()
    this.currentOutStream = null
    this.currentFileSize = 0
    this.openStream = false
  }

  _writeStreamFinishHandler (cb) {
    this.finishedWriteStreams++
    if (this.partitionNum === this.finishedWriteStreams) {
      cb(this.outStreams)
    }
  }

  write (chunk) {
    if (!this.openStream) {
      this.currentOutStream = this._outStreamCreate(this.partitionNum)
      this.currentOutStream.on('finish', this._writeStreamFinishHandler)
      this.outStreams.push(this.currentOutStream)
      this.partitionNum++
      this.openStream = true
    }

    this.currentOutStream.write(chunk + endOfLine)
    this.currentFileSize += chunk.length

    if (this.currentFileSize >= this.partitionStreamSize || chunk === `${endOfLine}`) {
      this._endCurrentWriteStream()
    }
  }

  end () {
    // only attempt to close stream if it's open
    if (this.openStream) {
      this._endCurrentWriteStream()
      this.emit('results', this.partitionNames)
    }
  }
}

module.exports = StreamSplitter
