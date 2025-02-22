const { EventEmitter } = require('events')
const { IterableMapper, IterableQueueMapperSimple } = require('@shutterstock/p-map-iterable')
const delay = require('delay')
const vm = require('vm')
const path = require('path')
const { getIsShuttingDown } = require('./shutdown')

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

  * offsetGenerator (limit, offset) {
    // This does not need to ever stop
    // We stop iterating this generator in __looper when we get an empty result
    while (true) {
      yield offset
      offset += limit
    }
  }

  async _loop (limit, offset, totalWrites) {
    return this.__looper(limit, offset, totalWrites)
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

  async __looper (limit, offset, totalWrites) {
    const ignoreErrors = this.options['ignore-errors'] === true
    const prefetcher = new IterableMapper(
      this.offsetGenerator(limit, offset),
      async (offset) => {
        const data = await this.get(limit, offset)
        return { data, offset }
      },
      {
        // Reading from ES scrolls or files both require reading in-order
        // so we set `concurrency` to 1 and do not allow it to be changed
        concurrency: 1,
        maxUnread: Math.max(5, 2 * (Math.min(this.options.concurrency, 20) || 1))
      }
    )
    // Async background flusher for writes
    // Default `concurrency: 1` will preserve the order of the writes
    // and just allow the next read / `applyModifiers` to begin
    // while the last write completes
    const flusher = new IterableQueueMapperSimple(async ({ data, offset }) => {
      // Write the data to the destination
      try {
        const writes = await this.set(data, limit, offset)
        totalWrites += writes
        if (data.length > 0) {
          this.log(`sent ${data.length} objects, ${offset} offset, to destination ${this.outputType}, wrote ${writes}`)
        }
      } catch (err) {
        if (!ignoreErrors) {
          // This will add the error to the `flusher.errors` array
          // We are just going to check that and throw the first one
          throw err
        }

        // Only emit the error if we didn't throw out
        // If we throw out, _loop will emit
        this.emit('error', err)
      }
    },
    {
      concurrency: this.options.concurrency || 1
    })

    // Iterate over the prefetched row blocks, in order
    // Typically there will always be a prefetched read so we will not wait here
    for await (const value of prefetcher) {
      if (getIsShuttingDown()) {
        this.log('Caught shutdown signal, waiting for writes to finish...')
        await flusher.onIdle()
        this.log('Writes finished, exiting...')
        return totalWrites
      }

      // Bail out if there is a write error and we are not ignoring them
      if (!ignoreErrors && flusher.errors && flusher.errors.length > 0) {
        throw flusher.errors[0]
      }

      const { data, offset } = value
      this.log(`got ${data.length} objects from source ${this.inputType} (offset: ${offset})`)

      if (data.length === 0) {
        // Finish all queued writes
        await flusher.onIdle()

        // Write the empty data to trigger file close
        // We only do this after the idle event so we do
        // not write close the destination before writes finish
        // (e.g. with `concurrency` > 1)
        await this.set(data, limit, offset)

        // Break out of the `while (true)` loop and end the process
        return totalWrites
      } else {
        // Only apply modifiers when we have non-zero length data
        this.applyModifiers(data)

        // Wait if flusher has no slots, otherwise no delay
        await flusher.enqueue({ data, offset })

        await delay(this.options.throttleInterval || 0)
      }
    }
  }
}

module.exports = TransportProcessor
