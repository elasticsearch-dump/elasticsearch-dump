const fs = require('fs')
const ini = require('ini')
const url = require('url')

module.exports = addAuth

function addAuth (urlToAddAuth, authFile) {
  const authConf = ini.parse(fs.readFileSync(authFile, 'utf-8'))
  if (authConf.user && authConf.password) {
    var authString = `${authConf.user}:${authConf.password}`
  } else {
    throw new Error('Malformed Auth File')
  }
  const urlObject = url.parse(urlToAddAuth)
  if (!urlObject.auth) {
    urlObject.auth = authString
    urlToAddAuth = url.format(urlObject)
  }
  return urlToAddAuth
}
