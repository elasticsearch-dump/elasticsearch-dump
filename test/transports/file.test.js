const should = require('should')
const { file: File } = require('../../lib/transports/file')
const fs = require('fs')
const path = require('path')

describe('File Transport', function () {
  let transport
  let testFile

  beforeEach(function () {
    testFile = path.join(__dirname, 'file.test.json')
    // create temp file
    fs.writeFileSync(testFile, '', 'utf8')
    transport = new File(
      { options: { overwrite: true } },
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

  it('should ensure all writes complete before closing stream', function (done) {
    const writes = []
    const expectedData = []

    // Multiple writes
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

    // Final empty write to close stream
    Promise.all(writes).then(() => {
      transport.set([], 0, 0, (err) => {
        should.not.exist(err)
        should.not.exist(transport.stream)

        // Verify file contents
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
  })
})
