const LosslessJSON = require('lossless-json')
const Decimal = require('decimal.js')

const _reviver = (key, value) => {
  if (value && value.isLosslessNumber) {
    return new Decimal(value.toString())
  }
  return value
}

const _replacer = (key, value) => {
  if (value instanceof Decimal) {
    return new LosslessJSON.LosslessNumber(value.toString())
  } else {
    return value
  }
}

const parse = (str) => {
  var result
  try {
    result = LosslessJSON.parse(str, _reviver)
  } catch (e) {
    throw new Error('failed to parse json (message: "' + e.message + '") - source: ' + JSON.stringify(str))
  }

  return result
}

const stringify = (json) => LosslessJSON.stringify(json, _replacer)

module.exports = {
  parse,
  stringify
}
