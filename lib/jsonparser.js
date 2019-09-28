const LosslessJSON = require('lossless-json')
const Decimal = require('decimal.js')
const _ = require('lodash')
const DECIMAL_REGEX = /^[-+]?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?$/

const _reviver = (key, value) => {
  if (value && value.isLosslessNumber) {
    return new Decimal(value.toString())
  }
  return value
}

const _replacer = (key, value) => {
  if (_.isString(value) && DECIMAL_REGEX.test(value) &&
    !isNaN(value) && parseFloat(value) > Number.MAX_SAFE_INTEGER) {
    return new LosslessJSON.LosslessNumber(value.replace(/^\+/, ''))
  } else if (value instanceof Decimal) {
    return new LosslessJSON.LosslessNumber(value.toString())
  } else {
    return value
  }
}

const parse = (str, configs) => {
  let result
  try {
    result = _.get(configs, 'options.support-big-int')
      ? LosslessJSON.parse(str, _reviver)
      : JSON.parse(str)
  } catch (e) {
    throw new Error(`failed to parse json (message: "${e.message}") - source: ${JSON.stringify(str)}`)
  }

  return result
}

const stringify = (json, configs) => _.get(configs, 'options.support-big-int')
  ? LosslessJSON.stringify(json, _replacer)
  : JSON.stringify(json)

module.exports = {
  parse,
  stringify
}
