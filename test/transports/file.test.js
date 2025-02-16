const should = require('should')
const { file: File } = require('../../lib/transports/file')
const fs = require('fs')
const path = require('path')
const zlib = require('zlib')

describe('File Transport', function () {
  let transport
  let testFile
  let testDir

  beforeEach(function () {
    testDir = path.join(__dirname, '..', 'tmp')
    testFile = path.join(testDir, 'test.json')

    if (!fs.existsSync(testDir)) {
      fs.mkdirSync(testDir, { recursive: true })
    }

    transport = new File(
      {
        options: {
          overwrite: true,
          fsCompress: false
        },
        emit: () => {}
      },
      testFile,
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

  describe('set()', function () {
    it('should handle empty data with no pending writes', function (done) {
      transport.set([], 0, 0, function (err) {
        should.not.exist(err)
        should.not.exist(transport.stream)
        done()
      })
    })

    it('should handle write to stdout', function (done) {
      transport.file = '$'
      transport.set([{ test: 'data' }], 0, 0, function (err, count) {
        should.not.exist(err)
        count.should.equal(1)
        done()
      })
    })

    it('should prevent overwriting existing files', function (done) {
      fs.writeFileSync(testFile, '')
      transport.parent.options.overwrite = false

      transport.set([{ test: 'data' }], 0, 0, function (err) {
        should.exist(err)
        err.message.should.containEql('already exists')
        done()
      })
    })
  })

  describe('stream handling', function () {
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
              content.should.containEql('"test":"compressed"')
              done()
            })
            .on('error', done)
        })
      })
    })

    it('should ensure all writes complete before closing', function (done) {
      const writes = []
      const expectedData = []

      for (let i = 0; i < 5; i++) {
        const data = [{ test: `data-${i}` }]
        expectedData.push(...data)
        writes.push(new Promise(resolve => {
          transport.set(data, 0, 0, (err, count) => {
            should.not.exist(err)
            count.should.equal(1)
            resolve()
          })
        }))
      }

      Promise.all(writes)
        .then(() => {
          transport.set([], 0, 0, (err) => {
            should.not.exist(err)
            should.not.exist(transport.stream)

            const content = fs.readFileSync(testFile, 'utf8')
            const lines = content.trim().split('\n')
            lines.should.have.length(5)

            lines.forEach((line, i) => {
              const parsed = JSON.parse(line)
              parsed.should.deepEqual({ test: `data-${i}` })
            })

            done()
          })
        })
        .catch(done)
    })
  })
})
