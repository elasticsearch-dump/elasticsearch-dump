const { EventEmitter } = require('events')
const { default: PQueue } = require('p-queue')
const delay = require('delay')

class TransportProcessor extends EventEmitter {
  log (message) {
    if (typeof this.options.logger === 'function') {
      this.options.logger(message)
    } else if (this.options.toLog === true) {
      this.emit('log', message)
    }
  }

  validateOptions (required = ['input', 'output']) {
    const validationErrors = []

    required.forEach(v => {
      if (!this.options[v]) {
        validationErrors.push(`\`${v}\` is a required input`)
      }
    })

    return validationErrors
  }

  static getParams (query) {
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

        const overlappedIoPromise = this.set(data, limit, offset)
          .then(writes => {
            totalWrites += writes
            if (data.length > 0) {
              this.log(`sent ${data.length} objects to destination ${this.outputType}, wrote ${writes}`)
            }
          })
          .catch(err => {
            if (ignoreErrors) {
              return Promise.resolve()
            }

            this.emit('error', err)
            return Promise.reject(err)
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

module.exports = TransportProcessor
