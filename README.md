elasticdump
==================

[![Join the chat at https://gitter.im/taskrabbit/elasticsearch-dump](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/taskrabbit/elasticsearch-dump?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

Tools for moving and saving indicies.

![picture](https://raw.github.com/taskrabbit/elasticsearch-dump/master/elasticdump.jpg)

[![Nodei stats](https://nodei.co/npm/elasticdump.png?downloads=true)](https://npmjs.org/package/elasticdump)

[![Build Status](https://secure.travis-ci.org/taskrabbit/elasticsearch-dump.png?branch=master)](http://travis-ci.org/taskrabbit/elasticsearch-dump)  [![Code Climate](https://codeclimate.com/github/taskrabbit/elasticsearch-dump/badges/gpa.svg)](https://codeclimate.com/github/taskrabbit/elasticsearch-dump)

## Version `1.0.0` Warning!

Version `1.0.0` of Elasticdump changes the format of the files created by the dump.  Files created with version `0.x.x` of this tool are likely not to work with versions going forward.  To learn more about the breaking changes, vist the release notes for version [`1.0.0`](https://github.com/taskrabbit/elasticsearch-dump/releases/tag/v1.0.0).  If you recive an "out of memory" error, this is probaly the cause.

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

# Backup ALL indices, then use Bulk API to populate another ES cluster:
elasticdump \
  --all=true \
  --input=http://production-a.es.com:9200/ \
  --output=/data/production.json
elasticdump \
  --bulk=true \
  --input=/data/production.json \
  --output=http://production-b.es.com:9200/

# Backup the results of a query to a file
elasticdump \
  --input=http://production.es.com:9200/my_index \
  --output=query.json \
  --searchBody '{"query":{"term":{"username": "admin"}}}'
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

# Backup ALL indices, then use Bulk API to populate another ES cluster:
# Notice, the single `/` is required to specify all indices.
elasticdump \
  --all=true \
  --input=http://production-a.es.com:9200/api/search \
  --input-index=/ \
  --output=/data/production.json
elasticdump \
  --bulk=true \
  --input=/data/production.json \
  --output=http://production-b.es.com:9200/api/search \
  --output-index=/
```

### Docker install
If you prefer using docker to use elasticdump, you can clone this git repo and run :
```bash
docker build -t elasticdump .
```
Then you can use it just by :
- using `docker run --rm -ti elasticdump`
- remembering that you cannot use `localhost` or `127.0.0.1` as you ES host ;)
- you'll need to mount your file storage dir `-v <your dumps dir>:<your mount point>` to your docker container

Example:
```bash
# Copy an index from production to staging with mappings:
docker run --rm -ti elasticdump \
  --input=http://production.es.com:9200/my_index \
  --output=http://staging.es.com:9200/my_index \
  --type=mapping
docker run --rm -ti elasticdump \
  --input=http://production.es.com:9200/my_index \
  --output=http://staging.es.com:9200/my_index \
  --type=data

# Backup index data to a file (ie : stored in /tmp/myESdumps) :
docker run --rm -ti -v /tmp/myESdumps:/data elasticdump \
  --input=http://production.es.com:9200/my_index \
  --output=/data/my_index_mapping.json \
  --type=mapping
docker run --rm -ti -v /tmp/myESdumps:/data elasticdump \
  --input=http://production.es.com:9200/my_index \
  --output=/data/my_index.json \
  --type=data
```

## Options

```
elasticdump: Import and export tools for elasticsearch

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
                    How many objects to move in bulk per operation
                    limit is approximate for file streams
                    (default: 100)
--debug
                    Display the elasticsearch commands being used
                    (default: false)
--type
                    What are we exporting?
                    (default: data, options: [data, mapping])
--delete
                    Delete documents one-by-one from the input as they are
                    moved.  Will not delete the source index
                    (default: false)
--searchBody
                    Preform a partial extract based on search results
                    (when ES is the input,
                    (default: '{"query": { "match_all": {} } }'))
--sourceOnly
                    Output only the json contained within the document _source
                    Normal: {"_index":"","_type":"","_id":"", "_source":{SOURCE}}
                    sourceOnly: {SOURCE}
                    (default: false)
--all
                    Load/store documents from ALL indexes
                    (default: false)
--bulk
                    Leverage elasticsearch Bulk API when writing documents
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
--bulk-mode
                    The mode can be index, delete or update.
                    'index': Add or replace documents on the destination index.
                    'delete': Delete documents on destination index.
                    'update': Use 'doc_as_upsert' option with bulk update API to do partial update.
                    (default: index)
--bulk-use-output-index-name
                    Force use of destination index name (the actual output URL)
                    as destination while bulk writing to ES. Allows
                    leveraging Bulk API copying data inside the same
                    elasticsearch instance.
                    (default: false)
--timeout
                    Integer containing the number of milliseconds to wait for
                    a request to respond before aborting the request. Passed
                    directly to the request library. If used in bulk writing,
                    it will result in the entire batch not being written.
                    Mostly used when you don't care too much if you lose some
                    data when importing but rather have speed.
--skip
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
--inputTransport
                    Provide a custom js file to us as the input transport
--outputTransport
                    Provide a custom js file to us as the output transport
--toLog
                    When using a custom outputTransport, should log lines
                    be appended to the output stream?
                    (default: true, except for `$`)
--help
                    This page
```

## Elasticsearch's scan and scroll method
Elasticsearch provides a scan and scroll method to fetch all documents of an index. This method is much safer to use since
it will maintain the result set in cache for the given period of time. This means it will be a lot faster to export the data
and more important it will keep the result set in order. While dumping the result set in batches it won't export duplicate
documents in the export. All documents in the export will unique and therefore no missing documents.

NOTE: only works for output

## Notes

- this tool is likley to require Elasticsearch version 1.0.0 or higher
- elasticdump (and elasticsearch in general) will create indices if they don't exist upon import
- when exporting from elasticsearch, you can have export an entire index (`--input="http://localhost:9200/index"`) or a type of object from that index (`--input="http://localhost:9200/index/type"`).  This requires ElasticSearch 1.2.0 or higher
- If elasticsearch is in a sub-directory, index and type must be provided with a separate argument (`--input="http://localhost:9200/sub/directory --input-index=index/type"`). Using `--input-index=/` will include all indices and types.
- we are using the `put` method to write objects.  This means new objects will be created and old objects with the same ID will be updated
- the `file` transport will overwrite any existing files
- If you need basic http auth, you can use it like this: `--input=http://name:password@production.es.com:9200/my_index`
- if you choose a stdio output (`--output=$`), you can also request a more human-readable output with `--format=human`
- if you choose a stdio output (`--output=$`), all logging output will be suppressed
- when using the `--bulk` option, aliases will be ignored and the documents you write will be linked thier original index name.  For example if you have an alias "events" which contains "events-may-2015" and "events-june-2015" and you bulk dump from one ES cluster to another `elasticdump --bulk --import http://localhost:9200/events --output http://other-server:9200`, you will have the source indicies, "events-may-2015" and "events-june-2015", and not "events".

Inspired by https://github.com/crate/elasticsearch-inout-plugin and https://github.com/jprante/elasticsearch-knapsack

Built at [TaskRabbit](https://www.taskrabbit.com)
