let isShuttingDown = false

function getIsShuttingDown () {
  return isShuttingDown
}

function registerGracefulShutdown () {
  process.on('SIGINT', function () {
    if (!isShuttingDown) {
      console.error('Caught SIGINT, exiting...')
    }
    isShuttingDown = true
  })

  process.on('SIGTERM', function () {
    if (!isShuttingDown) {
      console.error('Caught SIGTERM, exiting...')
    }
    isShuttingDown = true
  })
}

module.exports = {
  getIsShuttingDown,
  registerGracefulShutdown
}
