const http = require('http')
const https = require('https')
const TransportProcessor = require('./lib/processor')
const vm = require('vm')
const { promisify } = require('util')
const ioHelper = require('./lib/ioHelper')
const url = require('url')

class ElasticDump extends TransportProcessor {
  constructor (input, output, options) {
    super()
    this.input = input
    this.output = output
    this.options = options
    this.modifiers = []

    if (output !== '$' && (this.options.toLog === null || this.options.toLog === undefined)) {
      this.options.toLog = true
    }

    this.validationErrors = this.validateOptions()

    if (options.maxSockets) {
      this.log(`globally setting maxSockets=${options.maxSockets}`)
      http.globalAgent.maxSockets = options.maxSockets
      https.globalAgent.maxSockets = options.maxSockets
    }

    ioHelper(this, 'input')
    ioHelper(this, 'output')

    if (this.options.type === 'data' && this.options.transform) {
      if (!(this.options.transform instanceof Array)) {
        this.options.transform = [this.options.transform]
      }
      this.modifiers = this.options.transform.map(transform => {
        if (transform[0] === '@') {
          return doc => {
            const filePath = transform.slice(1).split('?')
            const parsed = url.pathToFileURL(filePath[0])
            return require(parsed.pathname)(doc, ElasticDump.getParams(filePath[1]))
          }
        } else {
          const modificationScriptText = `(function(doc) { ${transform} })`
          return new vm.Script(modificationScriptText).runInThisContext()
        }
      })
    }
  }

  dump (callback, continuing, limit, offset, totalWrites) {
    if (this.validationErrors.length > 0) {
      this.emit('error', { errors: this.validationErrors })
      callback(new Error('There was an error starting this dump'))
      return
    }

    // promisify helpers
    this.get = promisify(this.output.get).bind(this.input)
    this.set = promisify(this.output.set).bind(this.output)

    if (!limit) { limit = this.options.limit }
    if (!offset) { offset = this.options.offset }
    if (!totalWrites) { totalWrites = 0 }

    if (continuing !== true) {
      this.log('starting dump')

      if (this.options.offset) {
        this.log(`Warning: offsetting ${this.options.offset} rows.`)
        this.log('  * Using an offset doesn\'t guarantee that the offset rows have already been written, please refer to the HELP text.')
      }
      if (this.modifiers.length) {
        this.log(`Will modify documents using these scripts: ${this.options.transform}`)
      }
    }

    this._loop(limit, offset, totalWrites)
      .then((totalWrites) => {
        if (typeof callback === 'function') { return callback(null, totalWrites) }
      }, (error) => {
        if (typeof callback === 'function') { return callback(error/*, totalWrites */) }
      })
  }
}

module.exports = ElasticDump
