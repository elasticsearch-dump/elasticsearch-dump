module.exports.parse = function (str) {
  var result
  try {
    result = JSON.parse(str)
  } catch (e) {
    throw new Error('failed to parse json (message: "' + e.message + '") - source: ' + JSON.stringify(str))
  }

  return result
}
