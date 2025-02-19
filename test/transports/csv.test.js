const should = require('should')
const { csv: CSV } = require('../../lib/transports/csv')
const fs = require('fs')
const path = require('path')
const zlib = require('zlib')

describe('CSV Transport', function () {
  let transport
  let testFile
  let testDir

  beforeEach(function () {
    testDir = path.join(__dirname, '..', 'tmp')
    testFile = path.join(testDir, 'test.csv')

    if (!fs.existsSync(testDir)) {
      fs.mkdirSync(testDir, { recursive: true })
    }

    transport = new CSV(
      {
        options: {
          overwrite: true,
          fsCompress: false,
          csvWriteHeaders: true,
          csvRowDelimiter: '\n',
          csvFirstRowAsHeaders: true,
          csvDelimiter: ',',
          csvQuoteChar: '"',
          csvEscapeChar: '"'
        },
        emit: () => { }
      },
      `csv://${testFile}`,
      {}
    )
  })

  afterEach(function (done) {
    if (fs.existsSync(testFile)) {
      fs.unlink(testFile, done)
    } else {
      done()
    }
  })

  it('should ensure all writes complete before closing stream', function (done) {
    const writes = []
    const expectedData = []

    // Multiple writes
    for (let i = 0; i < 5; i++) {
      const data = [
        {
          _index: 'source_index',
          _type: 'seeds',
          _id: i,
          _score: 1,
          _source: {
            key: `key-${i}`,
            _uuid: `${i}`
          }
        }
      ]
      expectedData.push(...data)
      writes.push(new Promise(resolve => {
        transport.set(data, 0, 0, (err, count) => {
          should.not.exist(err)
          count.should.equal(1)
          resolve()
        })
      }))
    }

    // Final empty write to close stream
    Promise.all(writes)
      .then(() => {
        transport.set([], 0, 0, (err) => {
          should.not.exist(err)
          should.not.exist(transport.stream)

          // Verify file contents
          const content = fs.readFileSync(testFile, 'utf8')
          const lines = content.trim().split('\n')
          lines.length.should.be.above(5) // Including header

          lines.slice(1).forEach((line, i) => {
            line.should.containEql(`key-${i}`)
          })

          done()
        })
      })
      .catch(done)
  })

  it('should handle compressed streams', function (done) {
    transport.parent.options.fsCompress = true
    const testData = [{ test: 'compressed' }]

    // First write data
    transport.set(testData, 0, 0, function (err) {
      should.not.exist(err)

      // Then close stream
      transport.set([], 0, 0, function (err) {
        should.not.exist(err)
        should.not.exist(transport.stream)

        // Now read and verify file
        const gunzip = zlib.createGunzip()
        const chunks = []

        fs.createReadStream(testFile)
          .pipe(gunzip)
          .on('data', chunk => chunks.push(chunk))
          .on('end', () => {
            const content = Buffer.concat(chunks).toString()
            content.should.containEql('compressed')
            done()
          })
          .on('error', done)
      })
    })
  })

  it('should handle split streams', function (done) {
    // Enable stream splitting with small file size
    transport.parent.options.fileSize = '100b' // Small size to force splits
    transport.shouldSplit = true

    // Create test data large enough to cause splits
    const testData = Array(10).fill().map((_, i) => ({
      _index: 'source_index',
      _type: 'seeds',
      _id: i,
      _score: 1,
      _source: {
        key: `key-${i}`,
        value: 'x'.repeat(50) // Add padding to force splits
      }
    }))

    let filesCreated = []
    // transport.streamSplitter.on('results', (files) => {
    //   filesCreated = files
    // })

    // Write data
    transport.set(testData, 0, 0, (err) => {
      should.not.exist(err)

      filesCreated = transport.streamSplitter.streamList.map(stream => stream.path)

      // Close stream
      transport.set([], 0, 0, (err) => {
        should.not.exist(err)
        should.not.exist(transport.stream)

        // Verify multiple files were created
        filesCreated.length.should.be.above(1)

        // Verify each file
        const verifications = filesCreated.map(file => {
          return new Promise((resolve, reject) => {
            fs.readFile(file, 'utf8', (err, content) => {
              if (err) return reject(err)

              // Each file should have CSV headers
              const lines = content.trim().split('\n')
              // key,value,@id,@index,@type
              lines[0].should.match(/^key,value,@id,@index,@type/) // Header check
              lines.length.should.be.above(1) // Data rows exist

              resolve()
            })
          })
        })

        Promise.all(verifications)
          .then(() => done())
          .catch(done)
          .finally(() => {
            filesCreated.map(file => fs.unlinkSync(file))
          })
      })
    })
  })

  it('should handle compressed split streams', function (done) {
    transport.parent.options.fileSize = '100b'
    transport.parent.options.fsCompress = true
    transport.shouldSplit = true
    transport.streamSplitter.compress = true

    const testData = Array(5).fill().map((_, i) => ({
      id: i,
      data: 'x'.repeat(50)
    }))

    let filesCreated = []

    transport.set(testData, 0, 0, (err) => {
      should.not.exist(err)

      filesCreated = transport.streamSplitter.streamList.map(stream => stream.path)

      transport.set([], 0, 0, (err) => {
        should.not.exist(err)
        should.not.exist(transport.stream)

        // Verify compressed split files
        const verifications = filesCreated.map(file => {
          return new Promise((resolve, reject) => {
            const chunks = []
            fs.createReadStream(file)
              .pipe(zlib.createGunzip())
              .on('data', chunk => chunks.push(chunk))
              .on('end', () => {
                const content = Buffer.concat(chunks).toString()
                content.should.match(/^id,data/) // Has headers
                content.should.match(/x{50}/) // Has data
                resolve()
              })
              .on('error', reject)
          })
        })

        Promise.all(verifications)
          .then(() => done())
          .catch(done)
          .finally(() => {
            filesCreated.map(file => fs.unlinkSync(file))
          })
      })
    })
  })
})
