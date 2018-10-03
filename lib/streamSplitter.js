/**
 * Created by ferron on 10/3/18 1:04 PM
 * adapted from split-file-stream library
 * URL : https://github.com/dannycho7/split-file-stream/blob/master/index.js
 * credit @dannycho7
 */

const fs = require('fs')
const bytes = require('bytes')
const generateFilePath = (rootFileName, numFiles) => `${rootFileName}.split-${numFiles}`

const _splitToStream = (outStreamCreate, fileStream, partitionStreamSize, callback) => {
  const outStreams = [], {highWaterMark: defaultChunkSize} = fileStream._readableState // eslint-disable-line one-var
  let currentOutStream
  let currentFileSize = 0
  let fileStreamEnded = false
  let finishedWriteStreams = 0
  let openStream = false
  let partitionNum = 0

  const endCurrentWriteStream = () => {
    currentOutStream.end()
    currentOutStream = null
    currentFileSize = 0
    openStream = false
  }

  const writeStreamFinishHandler = () => {
    finishedWriteStreams++
    if (fileStreamEnded && partitionNum === finishedWriteStreams) {
      callback(outStreams)
    }
  }

  fileStream.on('readable', () => {
    let chunk
    while ((chunk = fileStream.read(Math.min(partitionStreamSize - currentFileSize, defaultChunkSize))) !== null) {
      if (openStream === false) {
        currentOutStream = outStreamCreate(partitionNum)
        currentOutStream.on('finish', writeStreamFinishHandler)
        outStreams.push(currentOutStream)
        partitionNum++
        openStream = true
      }

      currentOutStream.write(chunk)
      currentFileSize += chunk.length

      if (currentFileSize === partitionStreamSize) {
        endCurrentWriteStream()
      }
    }
  })

  fileStream.on('end', () => {
    if (currentOutStream) {
      endCurrentWriteStream()
    }
    fileStreamEnded = true
  })
}

const split = (fileStream, maxFileSize, rootFilePath, callback) => {
  const partitionNames = []

  const outStreamCreate = (partitionNum) => {
    let filePath = generateFilePath(rootFilePath, partitionNum)
    return fs.createWriteStream(filePath)
  }

  _splitToStream(outStreamCreate, fileStream, bytes(maxFileSize), (fileWriteStreams) => {
    fileWriteStreams.forEach((fileWriteStream) => partitionNames.push(fileWriteStream['path']))
    callback(partitionNames)
  })
}

module.exports = {
  split,
  _splitToStream
}
