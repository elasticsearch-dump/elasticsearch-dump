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

  async _loop(limit, offset, totalWrites) {
    try {
      const total = await this.__looper(limit, offset, totalWrites)
      this.log(`Total Writes: ${total}`)
      this.log('dump complete')
      return total
    } catch (err) {
      this.emit('error', err)
      this.log(`Total Writes: ${totalWrites}`)
      this.log(`dump ended with error (get phase) => ${String(err)}`)
      throw err
    }
  }

  async __looper(limit, offset, totalWrites) {
    const ignoreErrors = this.options['ignore-errors'] === true
    const maxConcurrent = this.options.concurrency || 1
    const interval = this.options.throttleInterval || 0
    const activePromises = new Set()

    return new Promise((resolve, reject) => {
      const processChunk = async () => {
        try {
          const data = await new Promise((resolve, reject) => {
            this.input.get(limit, offset, (err, data) => {
              if (err && !ignoreErrors) return reject(err)
              if (err) this.emit('error', err)
              resolve(data)
            })
          })

          if (!data.length) {
            // Wait for all pending operations to complete
            await Promise.all(Array.from(activePromises))
            return resolve(totalWrites)
          }

          this.log(`got ${data.length} objects from source ${this.inputType} (offset: ${offset})`)
          this.applyModifiers(data)

          const writePromise = (async () => {
            try {
              const writes = await this.set(data, limit, offset)
              totalWrites += writes
              this.log(`sent ${data.length} objects to destination ${this.outputType}, wrote ${writes}`)
            } catch (err) {
              if (!ignoreErrors) throw err
              this.emit('error', err)
            } finally {
              activePromises.delete(writePromise)
            }
          })()

          activePromises.add(writePromise)

          // Control concurrency
          if (activePromises.size >= maxConcurrent) {
            await Promise.race(Array.from(activePromises))
          }

          offset += data.length
          await delay(interval)

          // Process next chunk
          processChunk()
        } catch (err) {
          if (!ignoreErrors) return reject(err)
          this.emit('error', err)
          processChunk()
        }
      }

      // Start processing
      processChunk()
    })
  }
}

module.exports = TransportProcessor
