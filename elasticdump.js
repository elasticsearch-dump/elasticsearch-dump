const http = require('http')
const https = require('https')
const { EventEmitter } = require('events')
const vm = require('vm')
const { promisify } = require('util')
const ioHelper = require('./lib/ioHelper')
const url = require('url')
const { default: PQueue } = require('p-queue')
const delay = require('delay')

const getParams = query => {
  if (!query) {
    return {}
  }

  return (/^[?#]/.test(query) ? query.slice(1) : query)
    .split('&')
    .reduce((params, param) => {
      const [key, value] = param.split('=')
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
            return require(parsed.pathname)(doc, getParams(filePath[1]))
          }
        } else {
          const modificationScriptText = `(function(doc) { ${transform} })`
          return new vm.Script(modificationScriptText).runInThisContext()
        }
      })
    }

    // promisify helpers
    this.get = promisify(this.output.get).bind(this.input)
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

    required.forEach(v => {
      if (!self.options[v]) {
        validationErrors.push(`\`${v}\` is a required input`)
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
        self.log(`Warning: offsetting ${self.options.offset} rows.`)
        self.log('  * Using an offset doesn\'t guarantee that the offset rows have already been written, please refer to the HELP text.')
      }
      if (self.modifiers.length) {
        self.log(`Will modify documents using these scripts: ${self.options.transform}`)
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
    const queue = new PQueue({
      concurrency: this.options.concurrency || Infinity,
      interval: this.options.concurrencyInterval || 0,
      intervalCap: this.options.intervalCap || Infinity,
      carryoverConcurrencyCount: this.options.carryoverConcurrencyCount || false
    })
    return this.__looper(limit, offset, totalWrites, queue)
      .then(totalWrites => {
        this.log(`Total Writes: ${totalWrites}`)
        this.log('dump complete')
        return totalWrites
      })
      .catch(err => {
        this.emit('error', err)
        this.log(`Total Writes: ${totalWrites}`)
        this.log(`dump ended with error (get phase) => ${String(err)}`)
        throw err
      })
  }

  async __looper (limit, offset, totalWrites, queue) {
    const ignoreErrors = this.options['ignore-errors'] === true
    const set = promisify(this.output.set).bind(this.output)

    return new Promise((resolve, reject) => {
      this.input.get(limit, offset, (err, data) => {
        if (err) {
          this.emit('error', err)
          if (!ignoreErrors) {
            return reject(err)
          }
        }

        this.log(`got ${data.length} objects from source ${this.inputType} (offset: ${offset})`)
        if (this.modifiers.length) {
          for (let i = 0; i < data.length; i++) {
            this.modifiers.forEach(modifier => {
              modifier(data[i])
            })
          }
        }

        const overlappedIoPromise = set(data, limit, offset)
          .then(writes => {
            totalWrites += writes
            if (data.length > 0) {
              this.log(`sent ${data.length} objects to destination ${this.outputType}, wrote ${writes}`)
            }
          })

        if (data.length === 0) {
          return queue.onIdle()
            .then(() => resolve(totalWrites))
            .catch(reject)
        } else {
          return queue.add(() => overlappedIoPromise)
            .then(() => {
              offset += data.length
              return delay(this.options.throttleInterval || 0)
                .then(() => {
                  return this.__looper(limit, offset, totalWrites, queue)
                    .then(resolve)
                })
            })
            .catch(reject)
        }
      })
    })
  }
}

module.exports = elasticdump
