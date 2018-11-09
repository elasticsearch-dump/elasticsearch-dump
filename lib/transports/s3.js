const util = require('util')
const endOfLine = require('os').EOL
const jsonParser = require('../jsonparser.js')
const UploadStream = require('s3-stream-upload')
const S3 = require('aws-sdk').S3
const {Readable} = require('stream')

class s3 {
  constructor (parent, file, options) {
    this.options = options
    this.parent = parent
    this.file = file
    this.lineCounter = 0
    this.stream = null
    this.elementsToSkip = 0
    this._s3 = new S3({
      accessKeyId: parent.options.s3AccessKeyId,
      secretAccessKey: parent.options.s3SecretAccessKey
    })
    this._reading = false
  }

  // accept callback
  // return (error, arr) where arr is an array of objects
  get (limit, offset, callback) {
    throw Error('Not Yet Implemented')
  }

  // accept arr, callback where arr is an array of objects
  // return (error, writes)
  set (data, limit, offset, callback) {
    const self = this
    const error = null
    let targetElem

    self.lineCounter = 0

    if (!self._reading) {
      self._reading = true

      self.stream = new Readable({
        read () {
          if (data.length === 0) {
            this.push(null)
          }
        }
      })

      self.stream.pipe(UploadStream(self._s3, {
        Bucket: self.parent.options.s3Bucket,
        Key: self.parent.options.s3RecordKey || `elastic_${new Date().toISOString().replace(/T/, ' ').replace(/\..+/, '')}_output.jsonl`
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
        return callback(null, self.lineCounter)
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

        self.lineCounter++
      })

      process.nextTick(() => callback(error, self.lineCounter))
    }
  }

  log (line) {
    if (!line || line === '') {
      this.stream.push(null)
    } else {
      this.stream.push(line + endOfLine)
    }
  }
}

module.exports = {
  s3
}
