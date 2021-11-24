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
  constructor (file, options, context) {
    super(options)
    this.file = file
    this._ctx = context
    this.compress = options.s3Compress
  }

  _outStreamCreate (partitionNum) {
    let _throughStream = new PassThrough()
    if (this.compress) {
      _throughStream = zlib.createGzip()
    }

    const params = s3urls.fromUrl(this.file)
    const Key = StreamSplitter.generateFilePath(params.Key, partitionNum, this.compress)
    _throughStream.pipe(
      UploadStream(this._ctx._s3, Object.assign({
        Bucket: params.Bucket,
        Key,
        ServerSideEncryption: this._ctx.parent.options.s3ServerSideEncryption,
        SSEKMSKeyId: this._ctx.parent.options.s3SSEKMSKeyId,
        ACL: this._ctx.parent.options.ACL,
        StorageClass: this._ctx.parent.options.s3StorageClass
      }, this._ctx.parent.options.s3Options))
    ).on('error', error => {
      this._ctx.parent.emit('error', error)
    }).on('finish', () => {
      this._ctx.parent.emit('log', `Uploaded ${Key}`)
    })

    return _throughStream
  }
}

module.exports = s3StreamSplitter
