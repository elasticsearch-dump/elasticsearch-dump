class template {
  constructor (parent, options) {
    this.options = options
    this.parent = parent
  }

  // accept callback
  // return (error, arr) where arr is an array of objects
  get (limit, offset, callback) {
    const error = null
    const data = []

    callback(error, data)
  }

  // accept arr, callback where writes is a count of objects written
  // return (error, writes)
  set (data, limit, offset, callback) {
    const error = null
    const writes = 0

    callback(error, writes)
  }
}

module.exports = {
  template
}
