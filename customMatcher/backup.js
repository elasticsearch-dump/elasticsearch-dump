const fs = require('fs')

function needBackup (indexName) {
  const targetFile = `/data/docker/backup/${indexName}.json`
  const fiveDaysAgo = new Date()
  fiveDaysAgo.setDate(fiveDaysAgo.getDate() - 5)

  const indexDate = new Date(indexName.split('-')[1] + '-' + indexName.split('-')[2] + '-' + indexName.split('-')[3])

  // Check if target file exists
  if (!fs.existsSync(targetFile)) {
    return true
  }

  // Check if index age is more than 5 days
  return indexDate < fiveDaysAgo
}

module.exports = {
  test: needBackup
}
