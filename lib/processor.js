const { EventEmitter } = require('events')
const { default: PQueue } = require('p-queue')
const delay = require('delay')
const vm = require('vm')
const path = require('path')

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

  static castArray (prop) {
    if (!(prop instanceof Array)) {
      return [prop]
    }
    return prop
  }

  generateModifiers (transforms) {
    return TransportProcessor.castArray(transforms).map(transform => {
      if (transform[0] === '@') {
        return doc => {
          const filePath = transform.slice(1).split('?')
          const resolvedFilePath = path.resolve(process.cwd(), filePath[0])
          return require(resolvedFilePath)(doc, TransportProcessor.getParams(filePath[1]))
        }
      } else {
        const modificationScriptText = `(function(doc) { ${transform} })`
        return new vm.Script(modificationScriptText).runInThisContext()
      }
    })
  }

  applyModifiers (data = [], modifiers = this.modifiers) {
    if (modifiers.length && data.length) {
      for (let i = 0; i < data.length; i++) {
        modifiers.forEach(modifier => {
          modifier(data[i])
        })
      }
    }
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

    while (true) {
      const readPromise = new Promise((resolve, reject) => {
        this.input.get(limit, offset, (err, data) => {
          if (err) {
            this.emit('error', err)
            if (!ignoreErrors) {
              // This will cause `await readPromise` to throw out of the loop and
              // out of the `__looper` function.
              return reject(err)
            }
          }
          resolve(data || [])
        })
      });
      
      const data = await readPromise

      this.log(`got ${data.length} objects from source ${this.inputType} (offset: ${offset})`)
      this.applyModifiers(data)

      const overlappedIoPromise = this.set(data, limit, offset).catch(err => {
        // Should always emit write errors just like we do for read errors
        this.emit('error', err)

        if (!ignoreErrors) {
          return Promise.resolve(0)
        }

        return Promise.reject(err)
      })
      // NOTE: this doesn't really do anything as `queue.add()` returns only when the passed promise resolves,
      // which is evidenced by the fact that `queue.add()` returns the resolved value of the passed promise
      const writes = await queue.add(() => overlappedIoPromise)
      totalWrites += writes
      if (data.length > 0) {
        this.log(`sent ${data.length} objects to destination ${this.outputType}, wrote ${writes}`)
      }

      if (data.length === 0) {
        await queue.onIdle()
        // Break out of the `while (true)` loop and end the process
        return totalWrites
      } else {
        offset += data.length

        await delay(this.options.throttleInterval || 0)
      }
    }
  }
}

module.exports = TransportProcessor
