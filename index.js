var elasticdump = require('./elasticdump').elasticdump;

var options = {
    input: 'http://192.168.20.15:9200/',
    output: 'http://192.168.20.100/es/',
    'input-index': 'pat_tw_v6',
    'output-index': 'xxx',
    type: 'data',

    bulk: true,
    'bulk-use-output-index-name': true,
    limit: 100,

    offset: 0,
    debug: false,
    delete: false,
    all: false,
    maxSockets: null,
    inputTransport: null,
    outputTransport: null,
    searchBody: null,
    sourceOnly: false,
    jsonLines: false,
    format: '',
    'ignore-errors': false,
    scrollTime: '10m',
    timeout: null,
    skip: null,
    toLog: null,
};


var dumper = new elasticdump(options.input, options.output, options);

dumper.on('log', function (message) { log('log', message); });
dumper.on('debug', function (message) { log('debug', message); });
dumper.on('error', function (error) { log('log', 'Error Emitted => ' + (error.message || JSON.stringify(error))); });

dumper.dump(function (error, total_writes) {
    if (error) {
        process.exit(1);
    } else {
        process.exit(0);
    }
});

function log(level, msg) {
    console.log(level, msg);
}