module.exports.parse = function (str) {
  var result
  try {
    result = JSON.parse(str)
  } catch (e) {
    throw new Error('failed to parse json (message: "' + e.message + '") - source: ' + JSON.stringify(str))
  }

  return result
}

'use strict';
const LosslessJSON = require('lossless-json')
const Decimal = require('decimal.js')

// convert LosslessNumber to Decimal
function reviver (key, value) {
  if (value && value.isLosslessNumber) {
    return new Decimal(value.toString())
  }
  return value
}

// convert Decimal to LosslessNumber
function replacer (key, value) {
  if (value instanceof Decimal) {
    return new LosslessJSON.LosslessNumber(value.toString());
  }
  else {
    return value;
  }
}

module.exports.losslessParse = function(str) {
	return LosslessJSON.parse(str, reviver)
}

module.exports.losslessStringify = function(json) {
	 return LosslessJSON.stringify(json, replacer)
}
