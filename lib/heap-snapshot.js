const semver = require('semver')
const v8 = require('v8')
const path = require('path')

const createHeapSnapshot = () => {
  const filename = `pid-${process.pid}-${new Date().toISOString()}.heapsnapshot`
  const filenameWithPath = path.join(process.cwd(), filename)

  console.log(`\n\n\nwriting heapsnapshot: ${filenameWithPath}\n\n\n`)

  // Create the memory snapshot
  // https://dev.to/bengl/node-js-heap-dumps-in-2021-5akm
  // https://microsoft.github.io/PowerBI-JavaScript/modules/_node_modules__types_node_v8_d_._v8_.html#getheapsnapshot
  v8.writeHeapSnapshot(filenameWithPath)
}

const snap = () => {
  if (!semver.satisfies(process.version, '>=12')) {
    console.error('Node version needs to be >=12 to create heap snapshots')
    process.exit(1)
  }

  console.log(`Node.js version: ${process.version}`)
  console.log(`PID: ${process.pid}`)

  process.on('SIGUSR2', createHeapSnapshot)
}

module.exports = {
  snap
}
