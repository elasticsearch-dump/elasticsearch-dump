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
})
