elasticdump
==================

Tools for moving and saving indices.

![picture](https://raw.github.com/elasticsearch-dump/elasticsearch-dump/master/elasticdump.jpg)

---

[![Nodei stats](https://nodei.co/npm/elasticdump.png?downloads=true)](https://npmjs.org/package/elasticdump)
<br />
[![DockerHub Badge](https://dockeri.co/image/elasticdump/elasticsearch-dump)](https://hub.docker.com/r/elasticdump/elasticsearch-dump/)
[![DockerHub Badge](https://dockeri.co/image/taskrabbit/elasticsearch-dump)](https://hub.docker.com/r/taskrabbit/elasticsearch-dump/)

[![Build Status](https://secure.travis-ci.org/elasticsearch-dump/elasticsearch-dump.png?branch=master)](http://travis-ci.org/elasticsearch-dump/elasticsearch-dump)
[![Downloads](https://img.shields.io/npm/dm/elasticdump.svg)](https://npmjs.com/elasticdump)


## Version Warnings!

- Version `1.0.0` of Elasticdump changes the format of the files created by the dump.  Files created with version `0.x.x` of this tool are likely not to work with versions going forward.  To learn more about the breaking changes, vist the release notes for version [`1.0.0`](https://github.com/elasticsearch-dump/elasticsearch-dump/releases/tag/v1.0.0).  If you recive an "out of memory" error, this is probably or most likely the cause.
- Version `2.0.0` of Elasticdump removes the `bulk` options.  These options were buggy, and differ between versions of Elasticsearch.  If you need to export multiple indexes, look for the `multielasticdump` section of the tool.
- Version `2.1.0` of Elasticdump moves from using `scan/scroll` (ES 1.x) to just `scan` (ES 2.x).  This is a backwards-compatible change within Elasticsearch, but performance may suffer on Elasticsearch versions prior to 2.x.
- Version `3.0.0` of Elasticdump has the default queries updated to only work for ElasticSearch version 5+.  The tool *may* be compatible with earlier versions of Elasticsearch, but our version detection method may not work for all ES cluster topologies
- Version `5.0.0` of Elasticdump contains a breaking change for the s3 transport. _s3Bucket_ and _s3RecordKey_ params are no longer supported please use s3urls instead
- Version `6.1.0` and higher of Elasticdump contains a change to the upload/dump process. This change allows for overlapping promise processing. The benefit of which is improved performance due increased parallel processing, but a side-effect exists where-by records (data-set) aren't processed in a sequential order (the ordering is no longer guaranteed)

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

Elasticdump works by sending an `input` to an `output`. Both can be either an elasticsearch URL or a File.

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
  --searchBody="{\"query\":{\"term\":{\"username\": \"admin\"}}}"

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

# Backup templates to a file
elasticdump \
  --input=http://es.com:9200/template-filter \
  --output=templates.json \
  --type=template

# Import templates into ES
elasticdump \
  --input=./templates.json \
  --output=http://es.com:9200 \
  --type=template

# Split files into multiple parts
elasticdump \
  --input=http://production.es.com:9200/my_index \
  --output=/data/my_index.json \
  --fileSize=10mb

# Import data from S3 into ES (using s3urls)
elasticdump \
  --s3AccessKeyId "${access_key_id}" \
  --s3SecretAccessKey "${access_key_secret}" \
  --input "s3://${bucket_name}/${file_name}.json" \
  --output=http://production.es.com:9200/my_index

# Export ES data to S3 (using s3urls)
elasticdump \
  --s3AccessKeyId "${access_key_id}" \
  --s3SecretAccessKey "${access_key_secret}" \
  --input=http://production.es.com:9200/my_index \
  --output "s3://${bucket_name}/${file_name}.json"

# Import data from MINIO (s3 compatible) into ES (using s3urls)
elasticdump \
  --s3AccessKeyId "${access_key_id}" \
  --s3SecretAccessKey "${access_key_secret}" \
  --input "s3://${bucket_name}/${file_name}.json" \
  --output=http://production.es.com:9200/my_index
  --s3ForcePathStyle true
  --s3Endpoint https://production.minio.co

# Export ES data to MINIO (s3 compatible) (using s3urls)
elasticdump \
  --s3AccessKeyId "${access_key_id}" \
  --s3SecretAccessKey "${access_key_secret}" \
  --input=http://production.es.com:9200/my_index \
  --output "s3://${bucket_name}/${file_name}.json"
  --s3ForcePathStyle true
  --s3Endpoint https://production.minio.co
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

```

### Docker install
If you prefer using docker to use elasticdump, you can download this project from docker hub:
```bash
docker pull elasticdump/elasticsearch-dump
```
Then you can use it just by :
- using `docker run --rm -ti elasticdump/elasticsearch-dump`
- you'll need to mount your file storage dir `-v <your dumps dir>:<your mount point>` to your docker container

Example:
```bash
# Copy an index from production to staging with mappings:
docker run --rm -ti elasticdump/elasticsearch-dump \
  --input=http://production.es.com:9200/my_index \
  --output=http://staging.es.com:9200/my_index \
  --type=mapping
docker run --rm -ti elasticdump/elasticsearch-dump \
  --input=http://production.es.com:9200/my_index \
  --output=http://staging.es.com:9200/my_index \
  --type=data

# Backup index data to a file:
docker run --rm -ti -v /data:/tmp elasticdump/elasticsearch-dump \
  --input=http://production.es.com:9200/my_index \
  --output=/tmp/my_index_mapping.json \
  --type=data
```

If you need to run using `localhost` as your ES host:
```bash
docker run --net=host --rm -ti elasticdump/elasticsearch-dump \
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
--overwrite
                    Overwrite output file if it exists
                    (default: false)                    
--limit
                    How many objects to move in batch per operation
                    limit is approximate for file streams
                    (default: 100)
--size
                    How many objects to retrieve
                    (default: -1 -> no limit)
--concurrency
                    The maximum number of requests the can be made concurrently to a specified transport.
                    (default: 1)       
--concurrencyInterval
                    The length of time in milliseconds in which up to <intervalCap> requests can be made
                    before the interval request count resets. Must be finite.
                    (default: 5000)       
--intervalCap
                    The maximum number of transport requests that can be made within a given <concurrencyInterval>.
                    (default: 5)
--carryoverConcurrencyCount
                    If true, any incomplete requests from a <concurrencyInterval> will be carried over to
                    the next interval, effectively reducing the number of new requests that can be created
                    in that next interval.  If false, up to <intervalCap> requests can be created in the
                    next interval regardless of the number of incomplete requests from the previous interval.
                    (default: true)                                                                                       
--throttleInterval
                    Delay in milliseconds between getting data from an inputTransport and sending it to an
                    outputTransport.
                     (default: 1)
--debug
                    Display the elasticsearch commands being used
                    (default: false)
--quiet
                    Suppress all messages except for errors
                    (default: false)
--type
                    What are we exporting?
                    (default: data, options: [settings, analyzer, data, mapping, alias, template])
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
--searchWithTemplate
                    Enable to use Search Template when using --searchBody
                    If using Search Template then searchBody has to consist of "id" field and "params" objects
                    If "size" field is defined within Search Template, it will be overridden by --size parameter
                    See https://www.elastic.co/guide/en/elasticsearch/reference/current/search-template.html for 
                    further information
                    (default: false)
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
--scrollId
                    The last scroll Id returned from elasticsearch. 
                    This will allow dumps to be resumed used the last scroll Id &
                    `scrollTime` has not expired.
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
                    A method/function which can be called to modify documents
                    before writing to a destination. A global variable 'doc'
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
                    When using Amazon Elasticsearch Service protected by
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
--awsService
                    Sets the AWS service that the signature will be generated for
                    (default: calculated from hostname or host)
--awsRegion
                    Sets the AWS region that the signature will be generated for
                    (default: calculated from hostname or host)
--awsUrlRegex
                    Regular expression that defined valied AWS urls that should be signed
                    (default: ^https?:\\.*.amazonaws.com.*$)
--support-big-int   
                    Support big integer numbers
--big-int-fields   
                    Sepcifies a comma-seperated list of fields that should be checked for big-int support
                    (default '')
--retryAttempts  
                    Integer indicating the number of times a request should be automatically re-attempted before failing
                    when a connection fails with one of the following errors `ECONNRESET`, `ENOTFOUND`, `ESOCKETTIMEDOUT`,
                    ETIMEDOUT`, `ECONNREFUSED`, `EHOSTUNREACH`, `EPIPE`, `EAI_AGAIN`
                    (default: 0)
                    
--retryDelay   
                    Integer indicating the back-off/break period between retry attempts (milliseconds)
                    (default : 5000)            
--parseExtraFields
                    Comma-separated list of meta-fields to be parsed  
--fileSize
                    supports file splitting.  This value must be a string supported by the **bytes** module.     
                    The following abbreviations must be used to signify size in terms of units         
                    b for bytes
                    kb for kilobytes
                    mb for megabytes
                    gb for gigabytes
                    tb for terabytes
                    
                    e.g. 10mb / 1gb / 1tb
                    Partitioning helps to alleviate overflow/out of memory exceptions by efficiently segmenting files
                    into smaller chunks that then be merged if needs be.
--fsCompress
                    gzip data before sending outputting to file 
--s3AccessKeyId
                    AWS access key ID
--s3SecretAccessKey
                    AWS secret access key
--s3Region
                    AWS region
--s3Endpoint        
                    AWS endpoint can be used for AWS compatible backends such as
                    OpenStack Swift and OpenStack Ceph
--s3SSLEnabled      
                    Use SSL to connect to AWS [default true]
                    
--s3ForcePathStyle  Force path style URLs for S3 objects [default false]
                    
--s3Compress
                    gzip data before sending to s3  

--retryDelayBase
                    The base number of milliseconds to use in the exponential backoff for operation retries. (s3)
--customBackoff
                    Activate custom customBackoff function. (s3)
--tlsAuth
                    Enable TLS X509 client authentication
--cert, --input-cert, --output-cert
                    Client certificate file. Use --cert if source and destination are identical.
                    Otherwise, use the one prefixed with --input or --output as needed.
--key, --input-key, --output-key
                    Private key file. Use --key if source and destination are identical.
                    Otherwise, use the one prefixed with --input or --output as needed.
--pass, --input-pass, --output-pass
                    Pass phrase for the private key. Use --pass if source and destination are identical.
                    Otherwise, use the one prefixed with --input or --output as needed.
--ca, --input-ca, --output-ca
                    CA certificate. Use --ca if source and destination are identical.
                    Otherwise, use the one prefixed with --input or --output as needed.
--inputSocksProxy, --outputSocksProxy
                    Socks5 host address
--inputSocksPort, --outputSocksPort
                    Socks5 host port
--handleVersion
                    Tells elastisearch transport to handle the `_version` field if present in the dataset
                    (default : false)
--versionType
                    Elasticsearch versioning types. Should be `internal`, `external`, `external_gte`, `force`.
                    NB : Type validation is handled by the bulk endpoint and not by elasticsearch-dump
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

This package also ships with a second binary, `multielasticdump`.  This is a wrapper for the normal elasticdump binary, which provides a limited option set, but will run elasticdump in parallel across many indexes at once. It runs a process which forks into `n` (default your running host's # of CPUs) subprocesses running elasticdump.

The limited option set includes:

- `parallel`:   `os.cpus()`,
- `match`:      `'^.*$'`,
- `input`:      `null`,
- `output`:     `null`,
- `scrollTime`: `'10m'`,
- `timeout`: `null`,
- `limit`:      `100`,
- `offset`:     `0`,
- `direction`:   `dump`,
- `ignoreType`:   ``
- `includeType`:   ``
- `prefix`:   `'''`
- `suffix`:   `''`
- `interval`:     `1000`
- `searchbody`: `null`
- `transform`: `null`
- `support-big-int`: `false`
- `big-int-fields`: ``
- `ignoreChildError`: `false`

If the `--direction` is `dump`, which is the default, `--input` MUST be a URL for the base location of an ElasticSearch server (i.e. `http://localhost:9200`) and `--output` MUST be a directory. Each index that does match will have a data, mapping, and analyzer file created.

For loading files that you have dumped from multi-elasticsearch, `--direction` should be set to `load`, `--input` MUST be a directory of a multielasticsearch dump and `--output` MUST be a Elasticsearch server URL.

`--parallel` is how many forks should be run simultaneously and `--match` is used to filter which indexes should be dumped/loaded (regex).

`--ignoreType` allows a type to be ignored from the dump/load. Six options are supported. `data,mapping,analyzer,alias,settings,template`. Multi-type support is available, when used each type must be comma(,)-separated
and `interval` allows control over the interval for spawning a dump/load for a new index. For small indices this can be set to `0` to reduce delays and optimize performance
i.e analyzer,alias types are ignored by default

`--includeType` allows a type to be included in the dump/load. Six options are supported - `data,mapping,analyzer,alias,settings,template`. 

`ignoreChildError` allows multi-elasticdump to continue if a child throws an error.


New options, `--suffix` allows you to add a suffix to the index name being created e.g. `es6-${index}` and
`--prefix` allows you to add a prefix to the index name e.g. `${index}-backup-2018-03-13`.

## Usage Examples

```bash

# backup ES indices & all their type to the es_backup folder
multielasticdump \
  --direction=dump \
  --match='^.*$' \
  --input=http://production.es.com:9200 \
  --output=/tmp/es_backup

# Only backup ES indices ending with a prefix of `-index` (match regex). 
# Only the indices data will be backed up. All other types are ignored.
# NB: analyzer & alias types are ignored by default
multielasticdump \
  --direction=dump \
  --match='^.*-index$'\
  --input=http://production.es.com:9200 \
  --ignoreType='mapping,settings,template' \
  --output=/tmp/es_backup
```

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

- This tool is likely to require Elasticsearch version 1.0.0 or higher
- Elasticdump (and Elasticsearch in general) will create indices if they don't exist upon import
- When exporting from elasticsearch, you can export an entire index (`--input="http://localhost:9200/index"`) or a type of object from that index (`--input="http://localhost:9200/index/type"`). This requires ElasticSearch 1.2.0 or higher
- If the path to our elasticsearch installation is in a sub-directory, the index and type must be provided with a separate argument (`--input="http://localhost:9200/sub/directory --input-index=index/type"`).Using `--input-index=/` will include all indices and types.
- We can use the `put` method to write objects.  This means new objects will be created and old objects with the same ID be updated
- The `file` transport will not overwrite any existing files by default, it will throw an exception if the file already exists. You can make use of `--overwrite` instead.
- If you need basic http auth, you can use it like this: `--input=http://name:password@production.es.com:9200/my_index`
- If you choose a stdio output (`--output=$`), you can also request a more human-readable output with `--format=human`
- If you choose a stdio output (`--output=$`), all logging output will be suppressed
- If you are using Elasticsearch version 6.0.0 or higher the `offset` parameter is no longer allowed in the scrollContext
- ES 6.x.x & higher no longer support the `template` property for `_template`. All templates prior to ES 6.0 has to be upgraded to use `index_patterns`
- ES 7.x.x & higher no longer supports `type` property. All templates prior to ES 6.0 has to be upgraded to remove the type property
- ES 5.x.x ignores offset (from) parameter in the search body. All records will be returned
- ES 6.x.x [from](https://www.elastic.co/guide/en/elasticsearch/reference/6.8/breaking-changes-6.0.html#_scroll) parameter can no longer be used in the search request body when initiating a scroll
- Ensure JSON in the searchBody properly escaped to avoid parsing issues : https://www.freeformatter.com/json-escape.html
- Dropped support for Node.JS 8. Node.JS 10+ is now requireed.
Inspired by https://github.com/crate/elasticsearch-inout-plugin and https://github.com/jprante/elasticsearch-knapsack
