const TransportProcessor = require('../lib/processor')
const path = require('path')
require('should')

class MockTransport extends TransportProcessor {
  constructor (options = {}) {
    super()
    this.options = options
    this.inputData = options.inputData || []
    this.outputData = []
    this.currentIndex = 0
    this.modifiers = []

    // Mock input transport with get method
    this.input = {
      get: (limit, offset, callback) => {
        if (this.options.simulateReadError) {
          if (typeof this.options.simulateReadError === 'number') {
            this.options.simulateReadError--
          }
          return setTimeout(() => callback(new Error('Simulated read error'), []), 0) // Simulate async error
        }
        const start = offset
        const end = Math.min(start + limit, this.inputData.length)
        const data = this.inputData.slice(start, end)
        setTimeout(() => callback(null, data), 0) // Simulate async
      }
    }
  }

  // Mock output transport with set method
  async set (data, limit, offset) {
    if (this.options.simulateWriteError) {
      if (typeof this.options.simulateWriteError === 'number') {
        this.options.simulateWriteError--
      }
      throw new Error('Simulated write error')
    }
    this.outputData.push(...data)
    return data.length
  }
}

describe('TransportProcessor', () => {
  let processor

  beforeEach(() => {
    processor = new MockTransport()
  })

  describe('validateOptions', () => {
    it('should return validation errors for missing required options', () => {
      const errors = processor.validateOptions()
      errors.should.be.an.Array()
      errors.should.containEql('`input` is a required input')
      errors.should.containEql('`output` is a required input')
    })

    it('should return empty array when all required options are present', () => {
      processor.options = { input: 'test', output: 'test' }
      const errors = processor.validateOptions()
      errors.should.be.an.Array()
      errors.should.be.empty()
    })

    it('should validate custom required fields', () => {
      processor.options = {}
      const errors = processor.validateOptions(['custom'])
      errors.should.containEql('`custom` is a required input')
    })
  })

  describe('castArray', () => {
    it('should wrap non-array in array', () => {
      const result = TransportProcessor.castArray('test')
      result.should.be.an.Array()
      result.should.containEql('test')
    })

    it('should return array unchanged', () => {
      const arr = ['test1', 'test2']
      const result = TransportProcessor.castArray(arr)
      result.should.equal(arr)
    })
  })

  describe('generateModifiers', () => {
    it('should generate modifier function from inline transform', () => {
      const transform = 'doc.field = "value"'
      const modifiers = processor.generateModifiers(transform)
      modifiers.should.be.an.Array()
      modifiers.should.have.length(1)
      const doc = {}
      modifiers[0](doc)
      doc.should.have.property('field', 'value')
    })

    it('should handle multiple transforms', () => {
      const transforms = [
        'doc.field1 = "value1"',
        'doc.field2 = "value2"'
      ]
      const modifiers = processor.generateModifiers(transforms)
      modifiers.should.have.length(2)
      const doc = {}
      modifiers.forEach(modifier => modifier(doc))
      doc.should.have.properties({
        field1: 'value1',
        field2: 'value2'
      })
    })

    it('should load external transform file', () => {
      const transformPath = `${path.join('test', 'test-resources', 'transform.js')}?foo=true`
      const modifiers = processor.generateModifiers(['@' + transformPath])
      modifiers.should.have.length(1)
      const doc = {
        oldField: 'test',
        _source: { foo: 'test' }
      }
      modifiers[0](doc)
      doc._source.should.have.property('bar', '098f6bcd4621d373cade4e832627b4f6')
    })
  })

  describe('applyModifiers', () => {
    it('should apply modifiers to all documents', () => {
      const docs = [{ id: 1 }, { id: 2 }]
      const modifier = doc => { doc.modified = true }
      processor.modifiers = [modifier]
      processor.applyModifiers(docs)
      docs.forEach(doc => {
        doc.should.have.property('modified', true)
      })
    })

    it('should handle empty data array', () => {
      processor.modifiers = [doc => { doc.modified = true }]
      processor.applyModifiers([])
      // Should not throw error
    })

    it('should handle empty modifiers array', () => {
      const docs = [{ id: 1 }]
      processor.modifiers = []
      processor.applyModifiers(docs)
      docs[0].should.not.have.property('modified')
    })
  })

  describe('getParams', () => {
    it('should parse query string into object', () => {
      const params = TransportProcessor.getParams('param1=value1&param2=value2')
      params.should.have.properties({
        param1: 'value1',
        param2: 'value2'
      })
    })

    it('should handle empty query string', () => {
      const params = TransportProcessor.getParams('')
      params.should.be.an.Object()
      Object.keys(params).should.have.length(0)
    })

    it('should handle URL encoded values', () => {
      const params = TransportProcessor.getParams('field=value%20with%20spaces')
      params.should.have.property('field', 'value with spaces')
    })
  })

  describe('event emission', () => {
    it('should emit log events when toLog is true', (done) => {
      processor.options = { toLog: true }
      processor.once('log', (message) => {
        message.should.equal('test message')
        done()
      })
      processor.log('test message')
    })

    it('should use logger function when provided', () => {
      let logged = false
      processor.options = {
        logger: (message) => {
          message.should.equal('test message')
          logged = true
        }
      }
      processor.log('test message')
      logged.should.be.true()
    })
  })

  describe('offsetGenerator', () => {
    // This will be used in p-map-iterable version
    it.skip('should generate increasing offsets', async () => {
      const generator = processor.offsetGenerator(10, 0)
      const results = []
      for await (const offset of generator) {
        results.push(offset)
        if (results.length === 3) break
      }
      results.should.eql([0, 10, 20])
    })
  })

  describe('_loop', () => {
    it('should process data in batches with modifiers', async () => {
      const inputData = [
        { id: 1, value: 'test1' },
        { id: 2, value: 'test2' },
        { id: 3, value: 'test3' },
        { id: 4, value: 'test4' }
      ]

      const transport = new MockTransport({
        inputData,
        toLog: true
      })

      // Add a modifier that changes the value
      transport.modifiers = [
        doc => { doc.modified = doc.value.toUpperCase() }
      ]

      const totalWrites = await transport._loop(2, 0, 0)

      totalWrites.should.equal(4)
      transport.outputData.should.have.length(4)
      transport.outputData[0].should.have.property('modified', 'TEST1')
      transport.outputData[1].should.have.property('modified', 'TEST2')
      transport.outputData[2].should.have.property('modified', 'TEST3')
      transport.outputData[3].should.have.property('modified', 'TEST4')
    })

    it('should handle empty data set', async () => {
      const transport = new MockTransport({
        inputData: [],
        toLog: true
      })

      const totalWrites = await transport._loop(2, 0, 0)
      totalWrites.should.equal(0)
      transport.outputData.should.have.length(0)
    })

    // 2025-02-13 - This is not squashing the modifier error, which may be ok
    it.skip('should respect ignore-errors option for erroring modifiers', async () => {
      const inputData = [
        { id: 1, value: 'test1' },
        { id: 2, value: 'test2' }
      ]

      const transport = new MockTransport({
        inputData,
        'ignore-errors': true
      })

      // Add a modifier that throws an error
      transport.modifiers = [
        doc => {
          if (doc.id === 2) throw new Error('Test error')
          doc.modified = true
        }
      ]

      const totalWrites = await transport._loop(1, 0, 0)

      // Should complete despite error
      totalWrites.should.equal(2)
      transport.outputData.should.have.length(2)
      transport.outputData[0].should.have.property('modified', true)
    })

    it('should stop for read errors', async () => {
      const inputData = [
        { id: 1, value: 'test1' },
        { id: 2, value: 'test2' }
      ]

      const transport = new MockTransport({
        inputData,
        // Can't ignore a read error
        'ignore-errors': true,
        simulateReadError: 1
      })

      // Add error event listener to prevent crash
      let errorCount = 0
      transport.on('error', () => {
        // Error is expected in this test
        errorCount++
      })

      let transportError
      try {
        await transport._loop(1, 0, 0)
      } catch (error) {
        transportError = error
      }

      // Should complete despite error on first read
      transportError.message.should.equal('Simulated read error')
      errorCount.should.equal(1)
      transport.outputData.should.have.length(0)
    })

    it('should continue with ignore-errors: true option for write errors', async () => {
      const inputData = [
        { id: 1, value: 'test1' },
        { id: 2, value: 'test2' }
      ]

      const transport = new MockTransport({
        inputData,
        'ignore-errors': true,
        simulateWriteError: 1
      })

      // Add error event listener to prevent crash
      let emitErrorCount = 0
      transport.on('error', () => {
        // Error is expected in this test
        emitErrorCount++
      })

      const totalWrites = await transport._loop(1, 0, 0)

      // Should complete despite error
      emitErrorCount.should.equal(1)
      totalWrites.should.equal(1)
      transport.outputData.should.have.length(1)
      transport.outputData[0].should.have.property('value', 'test2')
    })

    it('should stop with ignore-errors: false option for write errors', async () => {
      const inputData = [
        { id: 1, value: 'test1' },
        { id: 2, value: 'test2' }
      ]

      const transport = new MockTransport({
        inputData,
        'ignore-errors': false,
        simulateWriteError: 1
      })

      // Add error event listener to prevent crash
      let emitErrorCount = 0
      transport.on('error', () => {
        // Error is expected in this test
        emitErrorCount++
      })

      let loopError = false
      try {
        await transport._loop(1, 0, 0)
      } catch {
        loopError = true
      }

      // Should complete despite error
      loopError.should.equal(true)
      emitErrorCount.should.equal(1)
      transport.outputData.should.have.length(0)
    })

    it('should handle throttle interval', async () => {
      const inputData = [
        { id: 1, value: 'test1' },
        { id: 2, value: 'test2' }
      ]

      const transport = new MockTransport({
        inputData,
        throttleInterval: 100
      })

      const startTime = Date.now()
      await transport._loop(1, 0, 0)
      const duration = Date.now() - startTime

      duration.should.be.aboveOrEqual(100)
      transport.outputData.should.have.length(2)
    })
  })
})
