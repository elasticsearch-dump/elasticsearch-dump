const _t = function (that, args) {
  'use strict'
  let p; let res = that; let i; const len = arguments.length; let cur
  for (i = 1; i < len; i += 1) {
    cur = arguments[i]
    for (p in cur) {
      res = res.replace(new RegExp('{' + p + '}', 'g'), cur[p])
    }
  }
  return res
}

const render = function (obj, options) {
  return JSON.parse(_t(JSON.stringify(obj), options))
}

function processTemplate (doc, options) {
  const template = doc.searchBody || {
    query: {
      range: {
        created: {
          gte: '{day_start}T00:00:00',
          lt: '{day_end}T23:59:59'
        }
      }
    },
    sort: [
      {
        id: {
          order: 'asc'
        }
      }
    ]
  }

  // muar return searchBody property
  doc.searchBody = render(template, options)
}

module.exports = function (body, options = {}) {
  processTemplate(body, options)
}
