const base = require('./base.js')
const util = require('util')
const fs = require('fs')
const { parse } = require('fast-csv')
const { PassThrough, pipeline } = require('stream')
const asyncPipeline = util.promisify(pipeline)
const _ = require('lodash')
const { fromCsvUrl } = require('../is-url.js')

class csv extends base {
  constructor (parent, file, options) {
    super(parent, file, options)
    this._reading = false

    // csv headers
    this._customHeaders = parent.options.csvCustomHeaders ? _.split(parent.options.csvCustomHeaders, ',') : []
    this._headers = this._customHeaders.length > 0 ? this.csvCustomHeaders : parent.options.csvFirstRowAsHeaders
    this._recordId = this.parent.options.csvIdColumn
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
      trim: this.parent.options.csvTrim,
      rtrim: this.parent.options.csvRTrim,
      ltrim: this.parent.options.csvLTrim,
      discardUnmappedColumns: this.parent.options.csvDiscardUnmappedColumns
    })

    if (!this.elementsToSkip) { this.elementsToSkip = offset }

    // converts the csv -> json object into
    // elasticdump object
    this.stream.transform(data => {
      const o = {
        _source: data
      }
      if (this._recordId) {
        o._id = data[this._recordId]
      }
      return o
    })

    const params = fromCsvUrl(this.file)

    this.metaStream = fs.createReadStream(params.url)

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
}

module.exports = {
  csv
}
