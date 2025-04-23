const fs = require('fs')
const ini = require('ini')
const url = require('url')

module.exports = addAuth

function addAuth (urlToAddAuth, authFile) {
  let authConf = {}
  if (authFile) {
    authConf = ini.parse(fs.readFileSync(authFile, 'utf-8'))
    if (!(authConf.user && authConf.password)) throw new Error('Malformed Auth File')
  } else {
    authConf.user = process.env.ELASTICDUMP_USERNAME
    authConf.password = process.env.ELASTICDUMP_PASSWORD
  }
  const urlObject = new URL(urlToAddAuth)
  if (!urlObject.username || !urlObject.password) {
    urlObject.username = authConf.user
    urlObject.password = authConf.password
    urlToAddAuth = url.format(urlObject)
  }
  return urlToAddAuth
}
