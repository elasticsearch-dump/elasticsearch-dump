const _ = require('lodash')
const { readFileSync } = require('fs')
const path = require('path')

class argv {
  constructor (config) {
    this.options = config.options || {}
    this.jsonParsedOpts = config.jsonParsedOpts ||
      [
        'searchBody',
        'input-headers',
        'headers',
        'output-headers',
        'input-params',
        'params',
        'output-params',
        's3Options',
        's3Configs'
      ]
    this.parseJSONOpts = config.parseJSONOpts
  }

  parse (argv, defaults, parseJSONOpts = false) {
    // parse passed options & use defaults otherwise
    for (const i in defaults) {
      this.options[i] = argv[i] || defaults[i]

      if (this.options[i] === 'true') { this.options[i] = true }
      if (this.options[i] === 'false') { this.options[i] = false }
      if (this.options[i] === 'Infinity') { this.options[i] = Infinity }
      if (this.options[i] === 'null') { this.options[i] = null }
      if (i === 'interval' && _.isNumber(argv[i])) {
        // special case to handle value == 0
        this.options[i] = argv[i]
      }

      if (i === 'bulkAction' && !/(index|update|delete|create)/.test(this.options[i])) {
        throw Error(`Unsupported action detected in bulkAction property : ${this.options[i]}`)
      }
    }

    if (parseJSONOpts || this.parseJSONOpts) {
      // parse whitelisted json formatted options
      for (const i of this.jsonParsedOpts) {
        if (/^(searchBody|s3Options|s3Configs)$/.test(i) && _.startsWith(this.options[i], '@')) {
          const resolvedFilePath = path.resolve(process.cwd(), this.options[i].substring(1))
          this.options[i] = readFileSync(resolvedFilePath).toString()
        }
        if (this.options[i]) { this.options[i] = JSON.parse(this.options[i]) }
      }
    }
  }

  log (type, message) {
    if (type === 'debug') {
      if (this.options.debug === true) {
        message = `${new Date().toUTCString()} [debug] | ${message}`
        console.log(message)
      } else {
        return false
      }
    } else if (type === 'error') {
      message = `${new Date().toUTCString()} | ${message}`
      console.error(message)
    } else if (this.options.quiet === false) {
      message = `${new Date().toUTCString()} | ${message}`
      console.log(message)
    } else {
      return false
    }
  }
}

module.exports = argv
