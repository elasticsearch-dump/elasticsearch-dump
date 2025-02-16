const should = require('should')
const { EventEmitter } = require('events')
const Base = require('../../lib/transports/base')

class MockStream extends EventEmitter {
  constructor () {
    super()
    this.paused = false
  }

  pause () { this.paused = true }
  resume () { this.paused = false }
}

describe('Base Transport', function () {
  let transport
  let mockParent

  beforeEach(function () {
    mockParent = {
      options: {
        sourceOnly: false,
        format: 'json'
      },
      emit: () => {}
    }
    transport = new Base(mockParent, 'test.file', {})
    transport.stream = new MockStream()
    transport._throughStream = new MockStream()
    transport.metaStream = new MockStream()
  })

  describe('stream control', function () {
    it('should pause all streams', function () {
      transport._pause()
      transport.stream.paused.should.be.true()
      transport._throughStream.paused.should.be.true()
      transport.metaStream.paused.should.be.true()
    })

    it('should resume all streams', function () {
      transport._pause()
      transport._resume()
      transport.stream.paused.should.be.false()
      transport._throughStream.paused.should.be.false()
      transport.metaStream.paused.should.be.false()
    })
  })

  describe('get()', function () {
    it('should handle stream end', function (done) {
      transport.streamEnded = true
      transport.setupGet = async () => {}
      transport.get(100, 0, function (err, data) {
        should.not.exist(err)
        data.should.be.Array()
        done()
      })
    })
  })

  describe('stream events', function () {
    it('should handle data events', function (done) {
      transport.thisGetLimit = 2
      transport.thisGetCallback = (err, data) => {
        should.not.exist(err)
        data.should.have.length(2)
        data[0].should.equal('elem2')
        data[1].should.equal('elem1')
        done()
      }

      transport.__setupStreamEvents()
      transport.stream.emit('data', 'elem1')
      transport.stream.emit('data', 'elem2')
    })

    it('should handle element skipping', function (done) {
      transport.elementsToSkip = 1
      transport.thisGetLimit = 1
      transport.thisGetCallback = (err, data) => {
        should.not.exist(err)
        data.should.have.length(1)
        data[0].should.equal('elem2')
        done()
      }

      transport.__setupStreamEvents()
      transport.stream.emit('data', 'elem1') // Should be skipped
      transport.stream.emit('data', 'elem2')
    })

    it('should emit stream errors', function (done) {
      const error = new Error('test error')
      mockParent.emit = (type, err) => {
        type.should.equal('error')
        err.should.equal(error)
        done()
      }

      transport.__setupStreamEvents()
      transport.stream.emit('error', error)
    })
  })

  describe('data handling', function () {
    beforeEach(function () {
      transport.log = () => {}
    })

    it('should handle source-only data', function () {
      mockParent.options.sourceOnly = true
      const data = [{ _source: { test: 'data' } }]

      const count = transport.__handleData(data)
      count.should.equal(1)
    })

    it('should handle human format', function () {
      mockParent.options.format = 'human'
      let loggedData = null
      transport.log = (data) => { loggedData = data }

      const data = [{ test: 'data' }]
      transport.__handleData(data)

      loggedData.should.containEql('test')
      loggedData.should.containEql('data')
    })
  })

  describe('batch completion', function () {
    it('should handle errors', function (done) {
      const error = new Error('test error')
      transport.completeBatch(error, (err) => {
        err.should.equal(error)
        done()
      })
    })

    it('should resume on skip with no data', function (done) {
      transport.elementsToSkip = 1
      transport._resume = done

      transport.completeBatch(null, () => {}, false)
    })

    it('should return buffered data', function (done) {
      transport.bufferedData = ['elem1', 'elem2']

      transport.completeBatch(null, (err, data) => {
        should.not.exist(err)
        data.should.have.length(2)
        data[0].should.equal('elem2')
        data[1].should.equal('elem1')
        done()
      })
    })
  })
})
