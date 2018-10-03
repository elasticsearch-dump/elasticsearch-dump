/**
 * Created by ferron on 10/3/18 1:04 PM
 * adapted from split-file-stream library
 * URL : https://github.com/dannycho7/split-file-stream/blob/master/index.js
 * credit by : @dannycho7
 */

const fs = require('fs')
const bytes = require('bytes')
const endOfLine = require('os').EOL
const generateFilePath = (rootFileName, numFiles) => `${rootFileName}.split-${numFiles}`
const defaultChunkSize = 64 * 1024

const _splitToStream = (outStreamCreate, chunk, partitionStreamSize, callback) => {
  const outStreams = []
  let currentOutStream
  let currentFileSize = 0
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
    if (partitionNum === finishedWriteStreams) {
      callback(outStreams)
    }
  }

  while (Math.min(partitionStreamSize - currentFileSize, defaultChunkSize)) {
    if (openStream === false) {
      currentOutStream = outStreamCreate(partitionNum)
      currentOutStream.on('finish', writeStreamFinishHandler)
      outStreams.push(currentOutStream)
      partitionNum++
      openStream = true
    }

    currentOutStream.write(chunk)
    currentFileSize += chunk.length

    if (currentFileSize === partitionStreamSize || chunk === `${endOfLine}`) {
      endCurrentWriteStream()
    }
  }
}

const split = (chunk, maxFileSize, rootFilePath, callback) => {
  const partitionNames = []

  const outStreamCreate = (partitionNum) => {
    let filePath = generateFilePath(rootFilePath, partitionNum)
    return fs.createWriteStream(filePath)
  }

  _splitToStream(outStreamCreate, chunk, bytes(maxFileSize), (fileWriteStreams) => {
    fileWriteStreams.forEach((fileWriteStream) => partitionNames.push(fileWriteStream['path']))
    callback(partitionNames)
  })
}

module.exports = {
  split,
  _splitToStream
}
