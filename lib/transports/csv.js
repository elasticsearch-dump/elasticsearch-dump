const base = require('./base.js')
const util = require('util')
const fs = require('fs')
const { parse, format } = require('fast-csv')
const { PassThrough, pipeline } = require('stream')
const asyncPipeline = util.promisify(pipeline)
const _ = require('lodash')
const { fromCsvUrl } = require('../is-url.js')
const jsonParser = require('../jsonparser.js')

class csv extends base {
  constructor (parent, file, options) {
    super(parent, file, options)
    this._reading = false

    // csv headers
    this._customHeaders = parent.options.csvCustomHeaders ? _.split(parent.options.csvCustomHeaders, ',') : []
    this._headers = this._customHeaders.length > 0 ? this.csvCustomHeaders : parent.options.csvFirstRowAsHeaders

    // csv custom columns
    this._csvIdColumn = this.parent.options.csvIdColumn
    this._csvIndexColumn = this.parent.options.csvIndexColumn
    this._csvTypeColumn = this.parent.options.csvTypeColumn

    // csv url parse
    this._fileObj = fromCsvUrl(this.file)
  }
    
  flatten (json) {
    return _.mapValues(json, (v) => jsonParser.stringify(v, this.parent))
  }

  unflatten (json) {
    return _.mapValues(json, (v) => jsonParser.parse(v, this.parent))
  }

  async setupGet (offset) {
    this.bufferedData = []
    this.stream = parse({
      headers: this._headers,
      delimiter: this.parent.options.csvDelimiter,
      renameHaders: this.parent.options.csvRenameHeaders,
      ignoreEmpty: this.parent.options.csvIgnoreEmpty,
      skipLines: this.parent.options.csvSkipLines,
      skipRows: this.parent.options.csvSkipRows,
      maxRows: this.parent.options.csvMaxRows,
      trim: this.parent.options.csvTrim,
      rtrim: this.parent.options.csvRTrim,
      ltrim: this.parent.options.csvLTrim,
      discardUnmappedColumns: this.parent.options.csvDiscardUnmappedColumns,
      quote: this.parent.options.csvQuoteChar,
      escape: this.parent.options.csvEscapeChar
    })

    if (!this.elementsToSkip) { this.elementsToSkip = offset }

    // converts the csv -> json object into
    // elasticdump object
    this.stream.transform(r => {
      const data = this.parent.options.csvHandleNestedData ? this.unflatten(r) : r

      const o = {
        _source: data
      }

      if (this._csvIdColumn) { o._id = data[this._csvIdColumn] }
      if (this._csvIndexColumn) { o._index = data[this._csvIndexColumn] }
      if (this._csvTypeColumn) { o._type = data[this._csvTypeColumn] }

      return o
    })

    this.metaStream = fs.createReadStream(this._fileObj.path)

    this.__setupStreamEvents()
    this._throughStream = new PassThrough()

    try {
      return await asyncPipeline(
        this.metaStream,
        this.stream
      )
    } catch (err) {
      throw new Error(util.inspect({ msg: 'Pipeline failed', err }))
    }
  }

  __handleData (data) {
    let lineCounter = 0
    data.forEach(elem => {
      let targetElem = elem
      if (elem._source) {
        targetElem = _.defaults(
          {
            ...elem._source
          },
          {
            [this._csvIdColumn || '@id']: elem._id,
            [this._csvIndexColumn || '@index']: elem._index,
            [this._csvTypeColumn || '@type']: elem._type
          })
      }

      this.log(targetElem)
      lineCounter++
    })
    return lineCounter
  }

  // accept arr, callback where arr is an array of objects
  // return (error, writes)
  set (data, limit, offset, callback) {
    const error = null
    let lineCounter = 0

    if (!this.stream) {
      // TODO: add options to append the file
      if (!this.parent.options.overwrite && fs.existsSync(this._fileObj.path)) {
        return callback(new Error(`File \`${this.file}\` already exists, quitting`))
      } else {
        const _throughStream = format({
          headers: this._headers,
          delimiter: this.parent.options.csvDelimiter,
          rowDelimiter: this.parent.options.csvRowDelimiter
        })

        // simple flat for csv
        if (this.parent.options.csvHandleNestedData) {
          _throughStream.transform(this.flatten.bind(this))
        }

        _throughStream.pipe(fs.createWriteStream(this._fileObj.path))
        this.stream = _throughStream
      }
    }

    if (data.length === 0) {
      this.stream.on('close', () => {
        delete this.stream
        return callback(null, lineCounter)
      })

      this.stream.end()
    } else {
      lineCounter += this.__handleData(data)
      process.nextTick(() => callback(error, lineCounter))
    }
  }

  log (line) {
    this.stream.write(line)
  }
}

module.exports = {
  csv
}
