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
          fsCompress: false,
          output: testFile
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

  describe('stream handling with split', function () {
    it('should handle split streams', function (done) {
      // Enable stream splitting with small file size
      transport.parent.options.fileSize = '100b' // Small size to force splits
      transport.parent.options.output = testFile
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

        filesCreated = transport.streamSplitter.streamList.map(stream => Object.keys(stream)[0])

        // Close stream
        transport.set([], 0, 0, (err) => {
          should.not.exist(err)
          should.not.exist(transport.stream)

          // Verify multiple files were created
          filesCreated.length.should.be.above(1)

          // Verify each file
          const verifications = filesCreated.map((file, index) => {
            return new Promise((resolve, reject) => {
              fs.readFile(file, 'utf8', (err, content) => {
                if (err) return reject(err)

                // Each file should have CSV headers
                const data = JSON.parse(content.trim())
                data._id.should.equal(index)
                data._source.key.should.equal(`key-${index}`)
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

        filesCreated = transport.streamSplitter.streamList.map(stream => Object.keys(stream)[0])

        transport.set([], 0, 0, (err) => {
          should.not.exist(err)
          should.not.exist(transport.stream)

          // Verify compressed split files
          const verifications = filesCreated.map((file, index) => {
            return new Promise((resolve, reject) => {
              const chunks = []
              fs.createReadStream(file)
                .pipe(zlib.createGunzip())
                .on('data', chunk => chunks.push(chunk))
                .on('end', () => {
                  const content = Buffer.concat(chunks).toString()

                  const data = JSON.parse(content)
                  data.id.should.equal(index)

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
})
