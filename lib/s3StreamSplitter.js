/**
 * Created by ferron on 10/3/18 1:04 PM
 * adapted from split-file-stream library
 * URL : https://github.com/dannycho7/split-file-stream/blob/master/index.js
 * credit to : @dannycho7
 */

const StreamSplitter = require('./streamSplitter')
const { PassThrough } = require('stream')
const UploadStream = require('s3-stream-upload')
const zlib = require('zlib')
const s3urls = require('s3urls')

class s3StreamSplitter extends StreamSplitter {
  constructor (file, options) {
    super(options)
    this.file = file
    this._s3 = null
    this.compress = options.s3Compress
  }

  _outStreamCreate (partitionNum) {
    let _throughStream = new PassThrough()
    if (this.compress) {
      _throughStream = zlib.createGzip()
    }

    const params = s3urls.fromUrl(this.file)
    const Key = StreamSplitter.generateFilePath(params.Key, partitionNum, this.compress)
    _throughStream.pipe(UploadStream(this._s3, {
      Bucket: params.Bucket,
      Key
    }))
      .on('error', function (err) {
        console.error(err)
      })
      .on('finish', function () {
        console.log(`Uploaded ${Key}`)
      })

    return _throughStream
  }
}

module.exports = s3StreamSplitter
