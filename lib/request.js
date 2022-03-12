const agent = require('socks5-http-client/lib/Agent')
const sslAgent = require('socks5-https-client/lib/Agent')
const fs = require('fs')

const applyProxy = (url, proxyHost, proxyPort) => {
  let reqAgent = agent

  if (url.indexOf('https://') === 0) {
    reqAgent = sslAgent
  }

  return {
    agentClass: reqAgent,
    agentOptions: {
      socksHost: proxyHost,
      socksPort: proxyPort
    }
  }
}

const applySSL = (props, ctx) => {
  const options = {}
  props.forEach(prop => {
    const val = ctx.parent.options[prop]
    if (val) {
      const newProp = prop.replace(/^input-/, '').replace(/^output-/, '')
      if (newProp === 'pass') {
        options.passphrase = val
      } else {
        options[newProp] = fs.readFileSync(val)
      }
    }
  })
  return options
}

module.exports = {
  applySSL,
  applyProxy
}
