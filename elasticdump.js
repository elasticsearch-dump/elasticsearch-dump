var util = require('util')
var http = require('http')
var https = require('https')
var path = require('path')
var EventEmitter = require('events').EventEmitter
var isUrl = require('./lib/is-url')
var vm = require('vm')
var addAuth = require('./lib/add-auth')

var elasticdump = function (input, output, options) {
  var self = this

  self.input = input
  self.output = output
  self.options = options
  self.modifiers = []

  if (self.options.toLog === null || self.options.toLog === undefined) {
    self.options.toLog = true
  }

  self.validationErrors = self.validateOptions()

  if (options.maxSockets) {
    self.log('globally setting maxSockets=' + options.maxSockets)
    http.globalAgent.maxSockets = options.maxSockets
    https.globalAgent.maxSockets = options.maxSockets
  }

  var InputProto
  if (self.options.input && !self.options.inputTransport) {
    if (isUrl(self.options.input)) {
      self.inputType = 'elasticsearch'
      if (self.options.httpAuthFile) {
        self.options.input = addAuth(self.options.input, self.options.httpAuthFile)
      }
    } else {
      self.inputType = 'file'
    }

    var inputOpts = {
      index: self.options['input-index'],
      headers: self.options['headers']
    }
    InputProto = require(path.join(__dirname, 'lib', 'transports', self.inputType))[self.inputType]
    self.input = (new InputProto(self, self.options.input, inputOpts))
  } else if (self.options.inputTransport) {
    self.inputType = String(self.options.inputTransport)
    InputProto = require(self.options.inputTransport)
    var inputProtoKeys = Object.keys(InputProto)
    self.input = (new InputProto[inputProtoKeys[0]](self, self.options.input, self.options['input-index']))
  }

  var OutputProto
  if (self.options.output && !self.options.outputTransport) {
    if (isUrl(self.options.output)) {
      self.outputType = 'elasticsearch'
      if (self.options.httpAuthFile) {
        self.options.output = addAuth(self.options.output, self.options.httpAuthFile)
      }
    } else {
      self.outputType = 'file'
      if (self.options.output === '$') { self.options.toLog = false }
    }

    var outputOpts = {
      index: self.options['output-index'],
      headers: self.options['headers']
    }
    OutputProto = require(path.join(__dirname, 'lib', 'transports', self.outputType))[self.outputType]
    self.output = (new OutputProto(self, self.options.output, outputOpts))
  } else if (self.options.outputTransport) {
    self.outputType = String(self.options.outputTransport)
    OutputProto = require(self.options.outputTransport)
    var outputProtoKeys = Object.keys(OutputProto)
    self.output = (new OutputProto[outputProtoKeys[0]](self, self.options.output, self.options['output-index']))
  }

  if (self.options.type === 'data' && self.options.transform) {
    if (!(self.options.transform instanceof Array)) {
      self.options.transform = [self.options.transform]
    }
    self.modifiers = self.options.transform.map(function (transform) {
      var modificationScriptText = '(function(doc) { ' + transform + ' })'
      return new vm.Script(modificationScriptText).runInThisContext()
    })
  }
}

util.inherits(elasticdump, EventEmitter)

elasticdump.prototype.log = function (message) {
  var self = this

  if (typeof self.options.logger === 'function') {
    self.options.logger(message)
  } else if (self.options.toLog === true) {
    self.emit('log', message)
  }
}

elasticdump.prototype.validateOptions = function () {
  var self = this
  var validationErrors = []

  var required = ['input', 'output']
  required.forEach(function (v) {
    if (!self.options[v]) {
      validationErrors.push('`' + v + '` is a required input')
    }
  })

  return validationErrors
}

elasticdump.prototype.dump = function (callback, continuing, limit, offset, totalWrites) {
  var self = this

  if (self.validationErrors.length > 0) {
    self.emit('error', {errors: self.validationErrors})
    callback(new Error('There was an error starting this dump'))
  } else {
    if (!limit) { limit = self.options.limit }
    if (!offset) { offset = self.options.offset }
    if (!totalWrites) { totalWrites = 0 }

    if (continuing !== true) {
      self.log('starting dump')

      if (self.options.offset) {
        self.log('Warning: offseting ' + self.options.offset + ' rows.')
        self.log("  * Using an offset doesn't guarantee that the offset rows have already been written, please refer to the HELP text.")
      }
      if (self.modifiers.length) {
        self.log('Will modify documents using these scripts: ' + self.options.transform)
      }
    }

    self.input.get(limit, offset, function (err, data) {
      if (err) { self.emit('error', err) }
      if (!err || (self.options['ignore-errors'] === true || self.options['ignore-errors'] === 'true')) {
        self.log('got ' + data.length + ' objects from source ' + self.inputType + ' (offset: ' + offset + ')')
        if (self.modifiers.length) {
          for (var i = 0; i < data.length; i++) {
            self.modifiers.forEach(function (modifier) {
              modifier(data[i])
            })
          }
        }
        self.output.set(data, limit, offset, function (err, writes) {
          var toContinue = true

          if (err) {
            self.emit('error', err)
            if (self.options['ignore-errors'] === true || self.options['ignore-errors'] === 'true') {
              toContinue = true
            } else {
              toContinue = false
            }
          } else {
            totalWrites += writes
            if (data.length > 0) {
              self.log('sent ' + data.length + ' objects to destination ' + self.outputType + ', wrote ' + writes)
              offset = offset + data.length
            }
          }

          if (data.length > 0 && toContinue) {
            self.dump(callback, true, limit, offset, totalWrites)
          } else if (toContinue) {
            self.log('Total Writes: ' + totalWrites)
            self.log('dump complete')
            if (typeof callback === 'function') { callback(null, totalWrites) }
          } else if (toContinue === false) {
            self.log('Total Writes: ' + totalWrites)
            self.log('dump ended with error (set phase)  => ' + String(err))
            if (typeof callback === 'function') { callback(err, totalWrites) }
          }
        })
      } else {
        self.log('Total Writes: ' + totalWrites)
        self.log('dump ended with error (get phase) => ' + String(err))
        if (typeof callback === 'function') { callback(err, totalWrites) }
      }
    })
  }
}

module.exports = elasticdump
