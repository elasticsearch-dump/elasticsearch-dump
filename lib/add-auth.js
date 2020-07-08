const fs = require('fs')
const ini = require('ini')
const url = require('url')

module.exports = addAuth

function addAuth (urlToAddAuth, authFile) {
  const authConf = ini.parse(fs.readFileSync(authFile, 'utf-8'))
  if (!(authConf.user && authConf.password)) throw new Error('Malformed Auth File')
  const urlObject = new URL(urlToAddAuth)
  if (!urlObject.username || !urlObject.password) {
    urlObject.username = authConf.user
    urlObject.password = authConf.password
    urlToAddAuth = url.format(urlObject)
  }
  return urlToAddAuth
}
