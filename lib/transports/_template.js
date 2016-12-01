var template = function (parent, options) {
  this.options = options
  this.parent = parent
}

// accept callback
// return (error, arr) where arr is an array of objects
template.prototype.get = function (limit, offset, callback) {
  var error = null
  var data = []

  callback(error, data)
}

// accept arr, callback where writes is a count of objects written
// return (error, writes)
template.prototype.set = function (data, limit, offset, callback) {
  var error = null
  var writes = 0

  callback(error, writes)
}

exports.template = template
