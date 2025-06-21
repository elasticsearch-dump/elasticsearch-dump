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
    this.transportType = 's3'
  }

  async setupGet (offset) {
    this.bufferedData = []
    this.stream = JSONStream.parse()
    this.currentBytePosition = 0
    this.maxRetries = 3
    this.retryCount = 0

    if (!this.elementsToSkip) {
      this.elementsToSkip = offset
    }

    await this._setupS3Stream()
  }

  async _setupS3Stream () {
    const params = s3urls.fromUrl(this.file)

    // Add Range header if we're resuming from a specific position
    if (this.currentBytePosition > 0) {
      params.Range = `bytes=${this.currentBytePosition}-`
    }

    this.metaStream = this._s3.getObject(params).createReadStream()

    // Track bytes read for resume capability
    let bytesRead = this.currentBytePosition
    this.metaStream.on('data', (chunk) => {
      bytesRead += chunk.length
      this.currentBytePosition = bytesRead
    })

    // Unified error handler for all stream errors
    const handleStreamError = async (err, errorType) => {
      if (this.retryCount < this.maxRetries) {
        this.parent.emit('log', `${errorType} error at byte ${this.currentBytePosition}, retrying... (${this.retryCount + 1}/${this.maxRetries}): ${err.message}`)

        this.retryCount++

        // Clean up current streams
        if (this.metaStream) {
          this.metaStream.destroy()
        }
        if (this._throughStream) {
          this._throughStream.destroy()
        }

        // Wait before retry with exponential backoff
        await new Promise(resolve => setTimeout(resolve, 1000 * this.retryCount))

        try {
          // Resume from current position
          await this._setupS3Stream()
        } catch (retryErr) {
          this.parent.emit('error', new Error(`Failed to resume stream after ${errorType.toLowerCase()} error: ${retryErr.message}`))
        }
      } else {
        this.parent.emit('error', new Error(`${errorType} failed after ${this.maxRetries} retries at byte ${this.currentBytePosition}: ${err.message}`))
      }
    }

    this.__setupStreamEvents()

    this._throughStream = new PassThrough()
    if (this.parent.options.s3Compress) {
      this._throughStream = zlib.createGunzip(this._zlibOptions)

      // Enhanced error handling for zlib decompression errors
      this._throughStream.on('error', (err) => {
        if (err.code === 'Z_BUF_ERROR' || err.code === 'Z_DATA_ERROR') {
          if (this.retryCount >= this.maxRetries) {
            this.parent.emit('error', new Error(`Decompression failed after ${this.maxRetries} retries at byte ${this.currentBytePosition}: ${err.message}. The file may be corrupted or not properly compressed.`))
            return
          }
          handleStreamError(err, 'Decompression')
        } else {
          // For non-retryable zlib errors, emit immediately without retry
          this.parent.emit('error', err)
        }
      })
    } else {
      // Error handling for PassThrough stream (non-compression cases)
      // PassThrough stream errors should not be retried
      this._throughStream.on('error', (err) => this.parent.emit('error', err))
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
