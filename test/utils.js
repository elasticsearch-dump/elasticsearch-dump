/**
 * Feb 16 2025 @ 1:18 AM
 * adapted from s3-stream-upload tests
 * URL https://github.com/jsantell/s3-stream-upload/blob/master/test/utils.js
 */

const fs = require('fs')

/**
 * Creates a bucket and deletes any files created from the tests.
 */
const before = (s3, bucket, keys, done) => {
  return createBucket(s3, bucket)
    .then(() => Promise.all(
      // Swallow deleteObject errors here incase the files don't yet exist
      keys.map(key => deleteObject(s3, bucket, key).catch(() => {}))
    )).then(() => { done() })
}
/**
 * Attempts to delete any files created from tests.
 */
const after = (s3, bucket, keys, done) => {
  return Promise.all(
    // Swallow deleteObject errors here incase the files don't yet exist
    keys.map(key => deleteObject(s3, bucket, key).catch(() => {}))
  ).then(() => { done() })
}

/**
 * Used to upload a file to s3/mocks3 and then subsequently pull down for analysis
 */
function uploadAndFetch (s3, stream, filename, bucket, key) {
  return new Promise((resolve, reject) => {
    getFileStream(filename)
      .pipe(stream)
      .on('error', reject)
      .on('finish', () => {
        resolve(getObject(s3, bucket, key))
      })
  })
}

function deleteObject (s3, bucket, key) {
  return new Promise((resolve, reject) => {
    s3.deleteObject({
      Bucket: bucket,
      Key: key
    }, (err, data) => {
      if (err) reject(err)
      else resolve(data)
    })
  })
}

function createBucket (s3, bucket) {
  return new Promise((resolve, reject) => {
    s3.createBucket({
      Bucket: bucket
    }, (err, data) => {
      if (err) reject(err)
      else resolve(data)
    })
  })
}

const getFileStream = (file) => {
  return fs.createReadStream(file)
}

const getFileBuffer = (file) => {
  return fs.readFileSync(file)
}

const getObject = (s3, bucket, key) => {
  return new Promise((resolve, reject) => {
    s3.getObject({
      Bucket: bucket,
      Key: key
    }, (err, data) => {
      if (err) reject(err)
      else resolve(data)
    })
  })
}

const convertJsonLinesToArray = (jsonLinesString) => {
  const jsonArray = []
  const lines = jsonLinesString.trim().split('\n')

  for (const line of lines) {
    if (line) { // Skip empty lines
      try {
        const jsonObject = JSON.parse(line)
        jsonArray.push(jsonObject)
      } catch (error) {
        console.error(`Error parsing JSON on line: ${line}`)
        // Handle the error as needed:
        // continue; // Skip the bad line
        // throw new Error(`Invalid JSON: ${error.message}`); // Stop processing
      }
    }
  }

  return jsonArray
}

module.exports = {
  before,
  after,
  getFileStream,
  getFileBuffer,
  uploadAndFetch,
  getObject,
  deleteObject,
  convertJsonLinesToArray
}
