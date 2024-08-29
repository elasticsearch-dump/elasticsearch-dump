const { SocksProxyAgent } = require('socks-proxy-agent')

const fs = require('fs')
const url = require('url')
const { isSocksUrl } = require('./is-url')

const createSocksUrl = (hostname, port) => {
  const protocol = 'socks'

  return url.format({
    protocol,
    hostname,
    port
  })
}

const applyProxy = (url, proxyHost, proxyPort) => {
  const agent = new SocksProxyAgent(
    isSocksUrl(proxyHost) ? proxyHost : createSocksUrl(proxyHost, proxyPort)
  )

  return {
    agent
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
