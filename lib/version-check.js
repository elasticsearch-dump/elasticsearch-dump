/**
 * Created by ferron on 3/25/21 11:04 AM
 */

const semver = require('semver')
const packageJSON = require('../package.json')

module.exports = () => {
  if (!semver.satisfies(process.version, packageJSON.engines.node)) {
    console.error(`Node version needs to satisfy ${packageJSON.engines.node}`)
    process.exit(1)
  }
}
