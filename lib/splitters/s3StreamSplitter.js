/**
 * Created by ferron on 10/3/18 1:04 PM
 * adapted from split-file-stream library
 * URL : https://github.com/dannycho7/split-file-stream/blob/master/index.js
 * credit to : @dannycho7
 */

const StreamSplitter = require('./streamSplitter')
const { PassThrough } = require('stream')
const zlib = require('zlib')
const s3urls = require('s3urls')
const jsonParser = require('../jsonparser.js')

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
    this._ctx._s3.upload({
      Bucket: params.Bucket,
      Key,
      Body: _throughStream
    }, (error, data) => {
      if (error) {
        return this._ctx.parent.emit('error', error)
      }
      return this._ctx.parent.emit('debug', jsonParser.stringify({ event: 'File Uploaded!', ...data }, this._ctx.parent))
    })

    return _throughStream
  }
}

module.exports = s3StreamSplitter
