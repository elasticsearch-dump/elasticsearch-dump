const JSONStream = require('JSONStream')
const { EOL } = require('os')
const base = require('./base.js')
const AWS = require('aws-sdk')
const initAws = require('../init-aws')
const StreamSplitter = require('../splitters/s3StreamSplitter')
const { Readable, PassThrough, finished } = require('stream')
const UploadStream = require('s3-stream-upload')
const zlib = require('zlib')
const s3urls = require('s3urls')
const util = require('util')

class s3 extends base {
  constructor (parent, file, options) {
    super(parent, file, options)
    this.streamSplitter = new StreamSplitter(file, parent.options, this)

    initAws(parent.options)
    this._s3 = new AWS.S3()
    this._reading = false
    this.pendingWrites = 0
    this.closeCallback = null
  }

  async setupGet (offset) {
    this.bufferedData = []
    this.stream = JSONStream.parse()

    if (!this.elementsToSkip) {
      this.elementsToSkip = offset
    }

    const params = s3urls.fromUrl(this.file)
    this.metaStream = this._s3.getObject(params).createReadStream()

    this.__setupStreamEvents()

    this._throughStream = new PassThrough()
    if (this.parent.options.s3Compress) {
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

    if (!this._reading) {
      this._reading = true

      if (!this.shouldSplit) {
        this.stream = new Readable({
          read () {
            if (data.length === 0) {
              this.push(null)
            }
          }
        })

        let _throughStream = new PassThrough()
        if (this.parent.options.s3Compress) {
          _throughStream = zlib.createGzip({ level: this.parent.options.compressionLevel })
        }

        const { Bucket, Key } = s3urls.fromUrl(this.file)

        const uploadStream = UploadStream(this._s3, Object.assign({
          Bucket,
          Key,
          ServerSideEncryption: this.parent.options.s3ServerSideEncryption,
          SSEKMSKeyId: this.parent.options.s3SSEKMSKeyId,
          ACL: this.parent.options.s3ACL,
          StorageClass: this.parent.options.s3StorageClass
        }, this.parent.options.s3Options))

        this.stream.pipe(_throughStream).pipe(uploadStream)
          .on('error', error => {
            this.parent.emit('error', error)
            return callback(error)
          })
          .on('finish', () => {
            this.parent.emit('log', 'Upload complete')
          })

        this._throughStream = _throughStream
        this.uploadStream = uploadStream
      }
    }

    if (data.length === 0) {
      this._reading = false

      // if (this.shouldSplit) {
      //   return this.streamSplitter.end(() => callback(null, lineCounter))
      // }

      if (this.pendingWrites > 0) {
        this.closeCallback = () => this.finalizeStream(callback, lineCounter)
      } else {
        this.finalizeStream(callback, lineCounter)
      }
    } else {
      this.pendingWrites++
      lineCounter += this.__handleData(data)
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
      const streamToWait = this.uploadStream || this._throughStream || this.stream
      finished(streamToWait, (err) => {
        delete this.stream
        delete this._throughStream
        delete this.fileStream
        this.closeCallback = null
        callback(err, lineCounter)
      })

      // Trigger stream closure
      this.stream.push(null)
    }
  }

  log (line) {
    if (this.shouldSplit) {
      this.streamSplitter.write(line)
    } else if (!line || line === '') {
      this.stream.push(null)
    } else {
      this.stream.push(line + EOL)
    }
  }
}

module.exports = {
  s3
}
