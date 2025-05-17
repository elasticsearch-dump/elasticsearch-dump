/**
 * Created by ferron on 10/3/18 1:04 PM
 * adapted from split-file-stream library
 * URL : https://github.com/dannycho7/split-file-stream/blob/master/index.js
 * credit to : @dannycho7
 */

const fs = require('fs')
const bytes = require('bytes')
const { EOL } = require('os')
const { EventEmitter } = require('events')
const { PassThrough } = require('stream')
const zlib = require('zlib')
const path = require('path')

class StreamSplitter extends EventEmitter {
  constructor (options, context) {
    super()
    this._ctx = context
    this.splitBySize = options.fileSize !== -1
    this.partitionStreamSize = this.splitBySize ? bytes(options.fileSize) : options.maxRows
    this.rootFilePath = options.output
    this.partitionNames = []
    this.currentOutStream = null
    this.currentFileSize = 0
    this.openStream = false
    this.partitionNum = 0
    this.compress = options.fsCompress
    this.compressionLevel = options.compressionLevel
    this.streamList = []
  }

  static generateFilePath (rootFileName, numFiles, compress) {
    const ext = path.extname(rootFileName)
    const dir = path.dirname(rootFileName)
    const baseName = path.basename(rootFileName, ext)
    return path.join(dir, `${baseName}.split-${numFiles}${ext}${compress ? '.gz' : ''}`)
  }

  _outStreamCreate (partitionNum) {
    let _throughStream = new PassThrough()
    if (this.compress) {
      _throughStream = zlib.createGzip({ level: this.compressionLevel })
    }
    const filePath = StreamSplitter.generateFilePath(this.rootFilePath, partitionNum, this.compress)
    const fileStream = fs.createWriteStream(filePath)
    _throughStream.pipe(fileStream)
    this.streamList.push({ [filePath]: fileStream })
    return _throughStream
  }

  _endCurrentWriteStream () {
    this.currentOutStream.end()
    this.currentOutStream = null
    this.currentFileSize = 0
    this.openStream = false
  }

  write (chunk) {
    if (!this.openStream) {
      this.currentOutStream = this._outStreamCreate(this.partitionNum)
      this.partitionNum++
      this.openStream = true
    }

    this.currentOutStream.write(chunk + EOL)

    if (this.splitBySize) {
      this.currentFileSize += chunk.length
    } else {
      this.currentFileSize++
    }

    if (this.currentFileSize >= this.partitionStreamSize || chunk === `${EOL}`) {
      this._endCurrentWriteStream()
    }
  }

  // Helper method to keep the logging logic in one place
  _logStreamCompletion (key) {
    switch (this._ctx.transportType) {
      case 's3':
        this._ctx.parent.emit('log', `Uploaded ${key}`)
        break
      case 'csv':
      case 'file':
        this._ctx.parent.emit('log', `Created ${key}`)
        break
    }
  }

  ensureFinished (callback) {
    // Make sure any open stream is properly ended first
    if (this.openStream) {
      this._endCurrentWriteStream()
    }

    const promises = this.streamList.map(object => {
      const key = Object.keys(object)[0]
      const stream = object[key]

      return new Promise((resolve, reject) => {
        // If stream is already finished, resolve immediately
        if (stream.writableFinished) {
          this._logStreamCompletion(key)
          return resolve()
        }

        // Otherwise wait for finish event
        stream.on('finish', () => {
          this._logStreamCompletion(key)
          resolve()
        })
        stream.on('error', (err) => {
          this._ctx.parent.emit('error', err)
          reject(err)
        })
      })
    })

    return Promise.all(promises)
      .then(() => callback())
      .catch(callback)
  }
}

module.exports = StreamSplitter
