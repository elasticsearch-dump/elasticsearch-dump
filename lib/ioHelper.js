var isUrl = require('./is-url')
var addAuth = require('./add-auth')
var path = require('path')

var getIo = function (elasticdump, type) {
  var EntryProto
  if (elasticdump.options[ type ] && !elasticdump[ type + 'Transport' ]) {
    if (isUrl(elasticdump.options[type])) {
      elasticdump[ type + 'Type' ] = 'elasticsearch'
      if (elasticdump.options.httpAuthFile) {
        elasticdump.options[type] = addAuth(elasticdump.options[type], elasticdump.options.httpAuthFile)
      }
    } else {
      elasticdump[ type + 'Type' ] = 'file'
    }

    var inputOpts = {
      index: elasticdump.options[ type + '-index' ],
      headers: elasticdump.options[ 'headers' ]
    }

    if (type === 'output') {
      Object.assign(inputOpts, {
        prefix: elasticdump.options['prefix'],
        suffix: elasticdump.options['suffix']
      })
    }

    EntryProto = require(path.join(__dirname, 'transports', elasticdump[ type + 'Type' ]))[elasticdump[ type + 'Type' ]]
    elasticdump[type] = (new EntryProto(elasticdump, elasticdump.options[type], inputOpts))
  } else if (elasticdump.options[ type + 'Transport' ]) {
    elasticdump[ type + 'Type' ] = String(elasticdump.options[ type + 'Transport' ])
    EntryProto = require(elasticdump.options[ type + 'Transport' ])
    var EntryProtoKeys = Object.keys(EntryProto)
    elasticdump[type] = (new EntryProto[EntryProtoKeys[0]](elasticdump, elasticdump.options[type], elasticdump.options[ type + '-index' ]))
  }
}

module.exports = getIo
