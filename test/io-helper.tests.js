const ioHelper = require('../lib/ioHelper')
require('should')

describe('ioHelper', () => {
  let elasticdump

  beforeEach(() => {
    elasticdump = {
      options: {}
    }
  })

  it('detects es transport', () => {
    elasticdump.options.output = 'http://es-output'

    ioHelper(elasticdump, 'output')

    elasticdump.outputType.should.equal('elasticsearch')
  })

  it('detects csv transport', () => {
    elasticdump.options.output = 'csv://my-export.csv'

    ioHelper(elasticdump, 'output')

    elasticdump.outputType.should.equal('csv')
  })

  it('detects s3 transport', () => {
    elasticdump.options.output = 's3://scaleway.bucket.io'

    ioHelper(elasticdump, 'output')

    elasticdump.outputType.should.equal('s3')
  })

  it('detects file transport', () => {
    elasticdump.options.output = './dump/%kuzzle.api-keys.template.json'

    ioHelper(elasticdump, 'output')

    elasticdump.outputType.should.equal('file')
  })
})
