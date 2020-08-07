const util = require('util')
const jsonParser = require('../jsonparser.js')

class base {
  constructor (parent, file, options) {
    this.options = options
    this.parent = parent
    this.file = file
    this.lineCounter = 0
    this.localLineCounter = 0
    this.stream = null
    this._throughStream = null
    this.metaStream = null
    this.elementsToSkip = 0

    // buffers data while it awaiting transport
    this.bufferedData = []
  }

  _resume () {
    [this.stream, this._throughStream, this.metaStream].forEach(stream => stream.resume())
  }

  _pause () {
    [this.metaStream, this._throughStream, this.stream].forEach(stream => stream.pause())
  }

  // accept callback
  // return (error, arr) where arr is an array of objects
  get (limit, offset, callback) {
    this.thisGetLimit = limit
    this.thisGetCallback = callback
    this.localLineCounter = 0

    if (this.lineCounter === 0) {
      this.setupGet(offset)
        .catch(e => {
          this.completeBatch(e, this.thisGetCallback)
        })
    } else {
      this._resume()
    }

    if (this.streamEnded) {
      this.completeBatch(null, this.thisGetCallback)
    }
  }

  async setupGet (offset) {
    throw new Error('Not Yet Implemented')
  }

  __setupStreamEvents () {
    this.stream.on('data', elem => {
      if (this.elementsToSkip > 0) {
        this.elementsToSkip--
      } else {
        this.bufferedData.push(elem)
      }

      this.localLineCounter++
      this.lineCounter++

      if (this.localLineCounter === this.thisGetLimit) {
        this.completeBatch(null, this.thisGetCallback)
      }
    })

    this.stream.on('error', e => {
      this.parent.emit('error', e)
    })

    this.stream.on('end', () => {
      this.streamEnded = true
      this.completeBatch(null, this.thisGetCallback, this.streamEnded)
    })
  }

  __handleData (data) {
    let lineCounter = 0
    data.forEach(elem => {
      let targetElem
      // Select _source if sourceOnly
      if (this.parent.options.sourceOnly === true) {
        targetElem = elem._source
      } else {
        targetElem = elem
      }

      if (this.parent.options.format && this.parent.options.format.toLowerCase() === 'human') {
        this.log(util.inspect(targetElem, false, 10, true))
      } else {
        this.log(jsonParser.stringify(targetElem, this.parent))
      }

      lineCounter++
    })
    return lineCounter
  }

  completeBatch (error, callback, streamEnded) {
    const data = []

    this._pause()

    if (error) { return callback(error) }

    // if we are skipping, have no data, and there is more to read we should continue on
    if (!streamEnded && this.elementsToSkip > 0 && this.bufferedData.length === 0) {
      return this._resume()
    }

    while (this.bufferedData.length > 0) {
      data.push(this.bufferedData.pop())
    }

    return callback(null, data)
  }

  set (data, limit, offset, callback) {
    throw new Error('Not Yet Implemented')
  }

  log (line) {
    throw new Error('Not Yet Implemented')
  }
}

module.exports = base
