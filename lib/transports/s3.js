const JSONStream = require('JSONStream')
const { EOL } = require('os')
const base = require('./base.js')
const UploadStream = require('s3-stream-upload')
const AWS = require('aws-sdk')
const StreamSplitter = require('../s3StreamSplitter')
const { Readable, PassThrough, pipeline } = require('stream')
const zlib = require('zlib')
const s3urls = require('s3urls')
const util = require('util')
const asyncPipeline = util.promisify(pipeline)

class s3 extends base {
  constructor (parent, file, options) {
    super(parent, file, options)
    this.streamSplitter = new StreamSplitter(file, parent.options)
    this.shouldSplit = !!parent.options.fileSize && parent.options.fileSize !== -1

    AWS.config.update({
      accessKeyId: parent.options.s3AccessKeyId,
      secretAccessKey: parent.options.s3SecretAccessKey,
      sslEnabled: parent.options.s3SSLEnabled,
      s3ForcePathStyle: parent.options.s3ForcePathStyle
    })
    if (parent.options.s3Endpoint != null) {
      AWS.config.update({
        endpoint: parent.options.s3Endpoint
      })
    }
    if (parent.options.s3Region != null) {
      AWS.config.update({
        region: parent.options.s3Region
      })
    }
    if (parent.options.debug) {
      AWS.config.update({
        logger: 'process.stdout'
      })
    }
    if (parent.options.retryAttempts > 0) {
      AWS.config.update({
        maxRetries: parent.options.retryAttempts
      })
    }
    if (parent.options.retryDelayBase > 0) {
      AWS.config.update({
        retryDelayOptions: { base: parent.options.retryDelayBase }
      })
    }
    if (parent.options.customBackoff) {
      AWS.config.update({
        retryDelayOptions: {
          customBackoff: retryCount => Math.max(retryCount * 100, 3000)
        }
      })
    }
    this._s3 = new AWS.S3()
    this.streamSplitter._s3 = this._s3
    this._reading = false
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
      return await asyncPipeline(
        this.metaStream,
        this._throughStream,
        this.stream
      )
    } catch (err) {
      throw new Error(util.inspect({ msg: 'Pipeline failed', err }))
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
          _throughStream = zlib.createGzip()
        }

        const params = s3urls.fromUrl(this.file)

        this.stream.pipe(_throughStream).pipe(UploadStream(this._s3, {
          Bucket: params.Bucket,
          Key: params.Key
        }))
          .on('error', function (err) {
            console.error(err)
          })
          .on('finish', function () {
            console.log('File uploaded!')
          })
      }
    }

    if (data.length === 0) {
      this._reading = false

      if (this.shouldSplit) {
        return this.streamSplitter.end()
      }

      // close readable stream
      this.stream.push(null)
      this.stream.on('close', () => {
        delete this.stream
        return callback(null, lineCounter)
      })
    } else {
      lineCounter += this.__handleData(data)
      process.nextTick(() => callback(error, lineCounter))
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
