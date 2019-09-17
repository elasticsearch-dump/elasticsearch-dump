const util = require('util')
const JSONStream = require('JSONStream')
const endOfLine = require('os').EOL
const jsonParser = require('../jsonparser.js')
const UploadStream = require('s3-stream-upload')
const AWS = require('aws-sdk')
const { Readable, PassThrough } = require('stream')
const zlib = require('zlib')
const s3urls = require('s3urls')

class s3 {
  constructor (parent, file, options) {
    this.options = options
    this.parent = parent
    this.file = file
    this.lineCounter = 0
    this.localLineCounter = 0
    this.stream = null
    this.elementsToSkip = 0

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
    this._reading = false
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
    const self = this

    self.bufferedData = []
    self.stream = JSONStream.parse()

    if (!self.elementsToSkip) { self.elementsToSkip = offset }

    const params = s3urls.fromUrl(self.file)
    self.metaStream = self._s3.getObject(params).createReadStream()

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
    if (self.parent.options.s3Compress) {
      _throughStream = zlib.createGunzip()
    }

    self.metaStream.pipe(_throughStream).pipe(self.stream)
  }

  // accept arr, callback where arr is an array of objects
  // return (error, writes)
  set (data, limit, offset, callback) {
    const self = this
    const error = null
    let targetElem

    let lineCounter = 0

    if (!self._reading) {
      self._reading = true

      self.stream = new Readable({
        read () {
          if (data.length === 0) {
            this.push(null)
          }
        }
      })

      let _throughStream = new PassThrough()
      if (self.parent.options.s3Compress) {
        _throughStream = zlib.createGzip()
      }

      const params = s3urls.fromUrl(self.file)

      self.stream.pipe(_throughStream).pipe(UploadStream(self._s3, {
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

    if (data.length === 0) {
      self._reading = false

      // close readable stream
      self.stream.push(null)
      self.stream.on('close', () => {
        delete self.stream
        return callback(null, lineCounter)
      })
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
    if (!line || line === '') {
      this.stream.push(null)
    } else {
      this.stream.push(line + endOfLine)
    }
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
}

module.exports = {
  s3
}
