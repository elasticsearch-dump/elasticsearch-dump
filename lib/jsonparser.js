const LosslessJSON = require('lossless-json')
const Decimal = require('big.js')
const _ = require('lodash')
// isValidNumber regex from lossless-json
// https://github.com/josdejong/lossless-json/blob/develop/lib/LosslessNumber.js#L153
// originally from: https://stackoverflow.com/questions/13340717/json-numbers-regular-expression
const DECIMAL_REGEX = /^[-]?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?$/

const _reviver = (bigIntFields = {}) => (key, value) => {
  if (!_.isEmpty(bigIntFields) && !bigIntFields[key]) {
    return value
  }
  if (value && value.isLosslessNumber) {
    return new Decimal(value.toString())
  }
  return value
}

const _replacer = (bigIntFields = {}) => (key, value) => {
  if (!_.isEmpty(bigIntFields) && !bigIntFields[key]) {
    return value
  }

  if (_.isString(value) && DECIMAL_REGEX.test(value) &&
    !isNaN(value) && parseFloat(value) > Number.MAX_SAFE_INTEGER) {
    return new LosslessJSON.LosslessNumber(value)
  } else if (value instanceof Decimal) {
    return new LosslessJSON.LosslessNumber(value.toString())
  } else {
    return value
  }
}

const _getBigIntFields = (configs) =>
  _.chain(_.get(configs, 'options.big-int-fields'))
    .split(',')
    .compact()
    .reduce((r, v) => { r[v] = true; return r }, {})
    .value()

const parse = (str, configs) => {
  let result
  try {
    result = _.get(configs, 'options.support-big-int')
      ? LosslessJSON.parse(str, _reviver.call(this, _getBigIntFields(configs)))
      : JSON.parse(str)
  } catch (e) {
    throw new Error(`failed to parse json (message: "${e.message}") - source: ${JSON.stringify(str)}`)
  }

  return result
}

const stringify = (json, configs) => _.get(configs, 'options.support-big-int')
  ? LosslessJSON.stringify(json, _replacer.call(this, _getBigIntFields(configs)))
  : JSON.stringify(json)

module.exports = {
  parse,
  stringify
}
