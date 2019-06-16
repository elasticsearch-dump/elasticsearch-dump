const http = require('http')
const https = require('https')
const { EventEmitter } = require('events')
const url = require('url')
const vm = require('vm')
const { promisify } = require('util')
const ioHelper = require('./lib/ioHelper')

const getParams = query => {
  if (!query) {
    return {}
  }

  return (/^[?#]/.test(query) ? query.slice(1) : query)
    .split('&')
    .reduce((params, param) => {
      let [key, value] = param.split('=')
      params[key] = value ? decodeURIComponent(value.replace(/\+/g, ' ')) : ''
      return params
    }, {})
}

class elasticdump extends EventEmitter {
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
      this.log('globally setting maxSockets=' + options.maxSockets)
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
            const parsed = url.parse(transform.slice(1))
            return require(parsed.pathname)(doc, getParams(parsed.query))
          }
        } else {
          const modificationScriptText = '(function(doc) { ' + transform + ' })'
          return new vm.Script(modificationScriptText).runInThisContext()
        }
      })
    }
  }

  log (message) {
    if (typeof this.options.logger === 'function') {
      this.options.logger(message)
    } else if (this.options.toLog === true) {
      this.emit('log', message)
    }
  }

  validateOptions () {
    const self = this
    const validationErrors = []

    const required = ['input']

    if (!self.options.s3Bucket) {
      required.push('output')
    }

    required.forEach(v => {
      if (!self.options[v]) {
        validationErrors.push('`' + v + '` is a required input')
      }
    })

    return validationErrors
  }

  dump (callback, continuing, limit, offset, totalWrites) {
    const self = this

    if (self.validationErrors.length > 0) {
      self.emit('error', { errors: self.validationErrors })
      callback(new Error('There was an error starting this dump'))
      return
    }

    if (!limit) { limit = self.options.limit }
    if (!offset) { offset = self.options.offset }
    if (!totalWrites) { totalWrites = 0 }

    if (continuing !== true) {
      self.log('starting dump')

      if (self.options.offset) {
        self.log('Warning: offsetting ' + self.options.offset + ' rows.')
        self.log('  * Using an offset doesn\'t guarantee that the offset rows have already been written, please refer to the HELP text.')
      }
      if (self.modifiers.length) {
        self.log('Will modify documents using these scripts: ' + self.options.transform)
      }
    }

    this._loop(limit, offset, totalWrites)
      .then((totalWrites) => {
        if (typeof callback === 'function') { return callback(null, totalWrites) }
      }, (error) => {
        if (typeof callback === 'function') { return callback(error/*, totalWrites */) }
      })
  }

  async _loop (limit, offset, totalWrites) {
    const self = this
    const get = promisify(this.input.get).bind(this.input)
    const set = promisify(this.output.set).bind(this.output)
    const ignoreErrors = self.options['ignore-errors'] === true || self.options['ignore-errors'] === 'true'

    let overlappedIoPromise
    for (;;) {
      let data
      try {
        data = await get(limit, offset)
      } catch (err) {
        self.emit('error', err)

        if (!ignoreErrors) {
          self.log('Total Writes: ' + totalWrites)
          self.log('dump ended with error (get phase) => ' + String(err))
          throw err
        }
      }

      self.log('got ' + data.length + ' objects from source ' + self.inputType + ' (offset: ' + offset + ')')
      if (self.modifiers.length) {
        for (let i = 0; i < data.length; i++) {
          self.modifiers.forEach(modifier => {
            modifier(data[i])
          })
        }
      }

      await overlappedIoPromise
      overlappedIoPromise = set(data, limit, offset)
        .then((writes) => {
          totalWrites += writes
          if (data.length > 0) {
            self.log('sent ' + data.length + ' objects to destination ' + self.outputType + ', wrote ' + writes)
          }
        }, (err) => {
          self.emit('error', err)

          if (!ignoreErrors) {
            self.log('Total Writes: ' + totalWrites)
            self.log('dump ended with error (set phase)  => ' + String(err))
            throw err
          }
        })

      if (data.length === 0) {
        break
      }
      offset = offset + data.length
    }
    await overlappedIoPromise // wait for the latest output.set() result

    self.log('Total Writes: ' + totalWrites)
    self.log('dump complete')
    return totalWrites
  }
}

module.exports = elasticdump
