elasticdump
==================

Tools for moving and saving indicies.

![picture](https://raw.github.com/taskrabbit/elasticsearch-dump/master/elasticdump.jpg)

---

[![Nodei stats](https://nodei.co/npm/elasticdump.png?downloads=true)](https://npmjs.org/package/elasticdump)
[![DockerHub Badge](https://dockeri.co/image/taskrabbit/elasticsearch-dump)](https://hub.docker.com/r/taskrabbit/elasticsearch-dump/)

[![Build Status](https://secure.travis-ci.org/taskrabbit/elasticsearch-dump.png?branch=master)](http://travis-ci.org/taskrabbit/elasticsearch-dump)  [![Code Climate](https://codeclimate.com/github/taskrabbit/elasticsearch-dump/badges/gpa.svg)](https://codeclimate.com/github/taskrabbit/elasticsearch-dump)
[![Join the chat at https://gitter.im/taskrabbit/elasticsearch-dump](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/taskrabbit/elasticsearch-dump?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

## Version Warnings!

- Version `1.0.0` of Elasticdump changes the format of the files created by the dump.  Files created with version `0.x.x` of this tool are likely not to work with versions going forward.  To learn more about the breaking changes, vist the release notes for version [`1.0.0`](https://github.com/taskrabbit/elasticsearch-dump/releases/tag/v1.0.0).  If you recive an "out of memory" error, this is probaly the cause.
- Version `2.0.0` of Elasticdump removes the `bulk` options.  These options were buggy, and differ between versions of Elasticsearch.  If you need to export multiple indexes, look for the `multielasticdump` section of the tool.
- Version `2.1.0` of Elasticdump moves from using `scan/scroll` (ES 1.x) to just `scan` (ES 2.x).  This is a backwards-compatible change within Elasticsearch, but performance may suffer on Elasticsearch versions prior to 2.x.
- Version `3.0.0` of Elasticdump has the default queries updated to only work for ElasticSearch version 5+.  The tool *may* be compatible with earlier versions of Elasticsearch, but our version detection method may not work for all ES cluster topologies

## Installing

(local)
```bash
npm install elasticdump
./bin/elasticdump
```

(global)
```bash
npm install elasticdump -g
elasticdump
```

## Use

### Standard Install

elasticdump works by sending an `input` to an `output`.  Both can be either an elasticsearch URL or a File.

Elasticsearch:
- format:  `{protocol}://{host}:{port}/{index}`
- example: `http://127.0.0.1:9200/my_index`

File:
- format:  `{FilePath}`
- example: `/Users/evantahler/Desktop/dump.json`

Stdio:
- format: stdin / stdout
- format: `$`

You can then do things like:

```bash
# Copy an index from production to staging with analyzer and mapping:
elasticdump \
  --input=http://production.es.com:9200/my_index \
  --output=http://staging.es.com:9200/my_index \
  --type=analyzer
elasticdump \
  --input=http://production.es.com:9200/my_index \
  --output=http://staging.es.com:9200/my_index \
  --type=mapping
elasticdump \
  --input=http://production.es.com:9200/my_index \
  --output=http://staging.es.com:9200/my_index \
  --type=data

# Backup index data to a file:
elasticdump \
  --input=http://production.es.com:9200/my_index \
  --output=/data/my_index_mapping.json \
  --type=mapping
elasticdump \
  --input=http://production.es.com:9200/my_index \
  --output=/data/my_index.json \
  --type=data

# Backup and index to a gzip using stdout:
elasticdump \
  --input=http://production.es.com:9200/my_index \
  --output=$ \
  | gzip > /data/my_index.json.gz

# Backup the results of a query to a file
elasticdump \
  --input=http://production.es.com:9200/my_index \
  --output=query.json \
  --searchBody '{"query":{"term":{"username": "admin"}}}'

# Copy a single shard data:
elasticdump \
  --input=http://es.com:9200/api \
  --output=http://es.com:9200/api2 \
  --params='{"preference" : "_shards:0"}'

# Backup aliases to a file
elasticdump \
  --input=http://es.com:9200/index-name/alias-filter \
  --output=alias.json \
  --type=alias

# Import aliases into ES
elasticdump \
  --input=./alias.json \
  --output=http://es.com:9200 \
  --type=alias
```

### Non-Standard Install

If Elasticsearch is not being served from the root directory the `--input-index` and
`--output-index` are required. If they are not provided, the additional sub-directories will
be parsed for index and type.

Elasticsearch:
- format:  `{protocol}://{host}:{port}/{sub}/{directory...}`
- example: `http://127.0.0.1:9200/api/search`

```bash
# Copy a single index from a elasticsearch:
elasticdump \
  --input=http://es.com:9200/api/search \
  --input-index=my_index \
  --output=http://es.com:9200/api/search \
  --output-index=my_index \
  --type=mapping

# Copy a single type:
elasticdump \
  --input=http://es.com:9200/api/search \
  --input-index=my_index/my_type \
  --output=http://es.com:9200/api/search \
  --output-index=my_index \
  --type=mapping

# Copy a single type:
elasticdump \
  --input=http://es.com:9200/api/search \
  --input-index=my_index/my_type \
  --output=http://es.com:9200/api/search \
  --output-index=my_index \
  --type=mapping

```

### Docker install
If you prefer using docker to use elasticdump, you can download this project from docker hub :
```bash
docker pull taskrabbit/elasticsearch-dump
```
Then you can use it just by :
- using `docker run --rm -ti taskrabbit/elasticsearch-dump`
- you'll need to mount your file storage dir `-v <your dumps dir>:<your mount point>` to your docker container

Example:
```bash
# Copy an index from production to staging with mappings:
docker run --rm -ti taskrabbit/elasticsearch-dump \
  --input=http://production.es.com:9200/my_index \
  --output=http://staging.es.com:9200/my_index \
  --type=mapping
docker run --rm -ti taskrabbit/elasticsearch-dump \
  --input=http://production.es.com:9200/my_index \
  --output=http://staging.es.com:9200/my_index \
  --type=data

# Backup index data to a file:
docker run --rm -ti -v /data:/tmp taskrabbit/elasticsearch-dump \
  --input=http://production.es.com:9200/my_index \
  --output=/tmp/my_index_mapping.json \
  --type=mapping
```

If you need to run using `localhost` as your ES host :
```bash
docker run --net=host --rm -ti taskrabbit/elasticsearch-dump \
  --input=http://staging.es.com:9200/my_index \
  --output=http://localhost:9200/my_index \
  --type=data
```

## Dump Format

The file format generated by this tool is line-delimited JSON files.  The dump file itself is not valid JSON, but each line is.  We do this so that dumpfiles can be streamed and appended without worrying about whole-file parser integrety.

For example, if you wanted to parse every line, you could do:
```
while read LINE; do jsonlint-py "${LINE}" ; done < dump.data.json
```

## Options

```
elasticdump: Import and export tools for elasticsearch
version: %%version%%

Usage: elasticdump --input SOURCE --output DESTINATION [OPTIONS]

--input
                    Source location (required)
--input-index
                    Source index and type
                    (default: all, example: index/type)
--output
                    Destination location (required)
--output-index
                    Destination index and type
                    (default: all, example: index/type)
--limit
                    How many objects to move in batch per operation
                    limit is approximate for file streams
                    (default: 100)

--size							How many objects to retrieve
										(default: -1 -> no limit)

--debug
                    Display the elasticsearch commands being used
                    (default: false)
--quiet
                    Suppress all messages except for errors
                    (default: false)
--type
                    What are we exporting?
                    (default: data, options: [settings, analyzer, data, mapping, alias])
--delete
                    Delete documents one-by-one from the input as they are
                    moved.  Will not delete the source index
                    (default: false)
--searchBody
                    Preform a partial extract based on search results
                    when ES is the input, default values are
                      if ES > 5
                        `'{"query": { "match_all": {} }, "stored_fields": ["*"], "_source": true }'`
                      else
                        `'{"query": { "match_all": {} }, "fields": ["*"], "_source": true }'`
--headers
                    Add custom headers to Elastisearch requests (helpful when
                    your Elasticsearch instance sits behind a proxy)
                    (default: '{"User-Agent": "elasticdump"}')
--params
                    Add custom parameters to Elastisearch requests uri. Helpful when you for example
                    want to use elasticsearch preference
                    (default: null)
--sourceOnly
                    Output only the json contained within the document _source
                    Normal: {"_index":"","_type":"","_id":"", "_source":{SOURCE}}
                    sourceOnly: {SOURCE}
                    (default: false)
--ignore-errors
                    Will continue the read/write loop on write error
                    (default: false)
--scrollTime
                    Time the nodes will hold the requested search in order.
                    (default: 10m)
--maxSockets
                    How many simultaneous HTTP requests can we process make?
                    (default:
                      5 [node <= v0.10.x] /
                      Infinity [node >= v0.11.x] )
--timeout
                    Integer containing the number of milliseconds to wait for
                    a request to respond before aborting the request. Passed
                    directly to the request library. Mostly used when you don't
                    care too much if you lose some data when importing
                    but rather have speed.
--offset
                    Integer containing the number of rows you wish to skip
                    ahead from the input transport.  When importing a large
                    index, things can go wrong, be it connectivity, crashes,
                    someone forgetting to `screen`, etc.  This allows you
                    to start the dump again from the last known line written
                    (as logged by the `offset` in the output).  Please be
                    advised that since no sorting is specified when the
                    dump is initially created, there's no real way to
                    guarantee that the skipped rows have already been
                    written/parsed.  This is more of an option for when
                    you want to get most data as possible in the index
                    without concern for losing some rows in the process,
                    similar to the `timeout` option.
                    (default: 0)
--noRefresh
                    Disable input index refresh.
                    Positive:
                      1. Much increase index speed
                      2. Much less hardware requirements
                    Negative:
                      1. Recently added data may not be indexed
                    Recommended to use with big data indexing,
                    where speed and system health in a higher priority
                    than recently added data.
--inputTransport
                    Provide a custom js file to use as the input transport
--outputTransport
                    Provide a custom js file to use as the output transport
--toLog
                    When using a custom outputTransport, should log lines
                    be appended to the output stream?
                    (default: true, except for `$`)
--transform
                    A javascript, which will be called to modify documents
                    before writing it to destination. global variable 'doc'
                    is available.
                    Example script for computing a new field 'f2' as doubled
                    value of field 'f1':
                        doc._source["f2"] = doc._source.f1 * 2;
                    May be used multiple times.
                    Additionally, transform may be performed by a module. See [Module Transform](#module-transform) below.
--awsChain
                    Use [standard](https://aws.amazon.com/blogs/security/a-new-and-standardized-way-to-manage-credentials-in-the-aws-sdks/) location and ordering for resolving credentials including environment variables, config files, EC2 and ECS metadata locations
                    _Recommended option for use with AWS_
--awsAccessKeyId
--awsSecretAccessKey
                    When using Amazon Elasticsearch Service proteced by
                    AWS Identity and Access Management (IAM), provide
                    your Access Key ID and Secret Access Key.
                    --sessionToken can also be optionally provided if using temporary credentials
--awsIniFileProfile
                    Alternative to --awsAccessKeyId and --awsSecretAccessKey,
                    loads credentials from a specified profile in aws ini file.
                    For greater flexibility, consider using --awsChain
                    and setting AWS_PROFILE and AWS_CONFIG_FILE
                    environment variables to override defaults if needed
--awsIniFileName
                    Override the default aws ini file name when using --awsIniFileProfile
                    Filename is relative to ~/.aws/
                    (default: config)
--support-big-int   Support big integer numbers
--help
                    This page
```

## Elasticsearch's Scroll API
Elasticsearch provides a [scroll](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-scroll.html) API to fetch all documents of an index starting form (and keeping) a consistent snapshot in time, which we use under the hood.  This method is safe to use for large exports since it will maintain the result set in cache for the given period of time.

NOTE: only works for `--output`

## Bypassing self-sign certificate errors

Set the environment `NODE_TLS_REJECT_UNAUTHORIZED=0` before running elasticdump

```bash
# An alternative method of passing environment variables before execution
# NB : This only works with linux shells
NODE_TLS_REJECT_UNAUTHORIZED=0 elasticdump --input="https://localhost:9200" --output myfile
```

## MultiElasticDump
This package also ships with a second binary, `multielasticdump`.  This is a wrapper for the normal elasticdump binary, which provides a limited option set, but will run elasticdump in parallel across many indexes at once.  It runs a process which forks into `n` (default your running host's # of CPUs) subprocesses running elasticdump.

The limited option set includes:

- `parallel`:   `os.cpus()`,
- `match`:      `'^.*$'`,
- `input`:      `null`,
- `output`:     `null`,
- `scrollTime`: `'10m'`,
- `limit`:      `100`,
- `offset`:     `0`,
- `direction`:   `dump`,
- `ignoreType`:   ``
- `prefix`:   `'''`
- `suffix`:   `''`
- `interval`:     `1000`

If the `--direction` is `dump`, which is the default, `--input` MUST be a URL for the base location of an ElasticSearch server (i.e. `http://localhost:9200`) and `--output` MUST be a directory. Each index that does match will have a data, mapping, and analyzer file created.

For loading files that you have dumped from multi-elasticsearch, `--direction` should be set to `load`, `--input` MUST be a directory of a multielasticsearch dump and `--output` MUST be a Elasticsearch server URL.

`--parallel` is how many forks should be run simultaneously and `--match` is used to filter which indexes should be dumped/loaded (regex).

`--ignoreType` allows a type to be ignored from the dump/load. Three options are supported. `data,mapping,analyzer`. Multi-type support is available, when used each type must be comma(,)-separated
and `interval` allows control over the interval for spawning a dump/load for a new index. For small indices this can be set to `0` to reduce delays and optimize performance

New options, `--suffix` allows you to add a suffix to the index name being created e.g. `es6-${index}` and
`--prefix` allows you to add a prefix to the index name e.g. `$index}-backup-2018-03-13`

## Module Transform

When specifying the `transform` option, prefix the value with `@` (a curl convention) to load the top-level function which is called with the document and the parsed arguments to the module.

Uses a pseudo-URL format to specify arguments to the module as follows. Given:

    elasticdump --transform='@./transforms/my-transform?param1=value&param2=another-value'

with a module at `./transforms/my-transform.js` with the following:

    module.exports = function (doc, options) {
        // do something to doc
    };

will load module `./transforms/my-transform.js', and execute the function with `doc` and `options` = `{"param1": "value", "param2": "another-value"}`.

An example transform for anonymizing data on-the-fly can be found in the `transforms` folder.

## Notes

- this tool is likely to require Elasticsearch version 1.0.0 or higher
- elasticdump (and elasticsearch in general) will create indices if they don't exist upon import
- when exporting from elasticsearch, you can have export an entire index (`--input="http://localhost:9200/index"`) or a type of object from that index (`--input="http://localhost:9200/index/type"`).  This requires ElasticSearch 1.2.0 or higher
- If elasticsearch is in a sub-directory, index and type must be provided with a separate argument (`--input="http://localhost:9200/sub/directory --input-index=index/type"`). Using `--input-index=/` will include all indices and types.
- we are using the `put` method to write objects.  This means new objects will be created and old objects with the same ID will be updated
- the `file` transport will not overwrite any existing files, it will throw an exception of the file already exists
- If you need basic http auth, you can use it like this: `--input=http://name:password@production.es.com:9200/my_index`
- if you choose a stdio output (`--output=$`), you can also request a more human-readable output with `--format=human`
- if you choose a stdio output (`--output=$`), all logging output will be suppressed
- if you are using Elasticsearch version 6.0.0 or higher the `offset` parameter is no longer allowed in the scrollContext

Inspired by https://github.com/crate/elasticsearch-inout-plugin and https://github.com/jprante/elasticsearch-knapsack

Built at [TaskRabbit](https://www.taskrabbit.com)
