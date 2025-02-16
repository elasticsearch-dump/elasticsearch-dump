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
      const data = await this.get(limit, offset)

      this.log(`got ${data.length} objects from source ${this.inputType} (offset: ${offset})`)

      // We always setup the write because data [] is used to close the stream
      const overlappedIoPromise = this.set(data, limit, offset).catch(err => {
        if (ignoreErrors) {
          // We only emit if continuing to run after errors
          // If stopping after errors then the catch in _loop will emit
          this.emit('error', err)
          return Promise.resolve(0)
        }

        return Promise.reject(err)
      })

      if (data.length === 0) {
        await queue.onIdle()
        // Trigger the close of the destination stream
        await overlappedIoPromise
        // Break out of the `while (true)` loop and end the process
        return totalWrites
      } else {
        this.applyModifiers(data)

        // NOTE: this doesn't really do anything as `queue.add()` returns only when the passed promise resolves,
        // which is evidenced by the fact that `queue.add()` returns the resolved value of the passed promise
        const writes = await queue.add(() => overlappedIoPromise)
        totalWrites += writes
        this.log(`sent ${data.length} objects to destination ${this.outputType}, wrote ${writes}`)

        offset += data.length

        await delay(this.options.throttleInterval || 0)
      }
    }
  }
}

module.exports = TransportProcessor
