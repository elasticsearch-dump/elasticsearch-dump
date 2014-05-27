elasticdump
==================

Tools for moving and saving indicies.

![picture](https://raw.github.com/taskrabbit/elasticsearch-dump/master/elasticdump.jpg)

[![Nodei stats](https://nodei.co/npm/elasticdump.png?downloads=true)](https://npmjs.org/package/elasticdump)

[![Build Status](https://secure.travis-ci.org/taskrabbit/elasticsearch-dump.png?branch=master)](http://travis-ci.org/taskrabbit/elasticsearch-dump)

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

elasticdump works by sending an `input` to an `output`.  Both can be either an elasticsearch URL or a File. 

Elasticsearch:
- format:  `{protocol}://{host}:{port}/{index}`
- example: `http://127.0.0.1:9200/my_index`

File:
- format:  `{FilePath}`
- example: `/Users/evantahler/Desktop/dump.json`

Stdio:
- format: stdin / stdout
- format: $

You can then do things like:

```bash
# Copy an index from production to staging: 
elasticdump --input=http://production.es.com:9200/my_index --output=http://staging.es.com:9200/my_index

# Backup an index to a file: 
elasticdump --input=http://production.es.com:9200/my_index --output=/data/my_index.json

# Backup and index to a gzip using stdout:
elasticdump --input=http://production.es.com:9200/my_index --output=$ | gzip > /data/my_index.json.gz

# Backup ALL indices, then use Bulk API to populate another ES cluster:
elasticdump --all=true --input=http://production-a.es.com:9200/ --output=/data/production.json
elasticdump --bulk=true --input=/data/production.json --output=http://production-b.es.com:9200/
```

## Options

- `--input` (required) (see above)
- `--output` (required) (see above)
- `--limit` how many ojbects to move in bulk per operation (default: 100)
- `--debug` display the elasticsearch commands being used (default: false)
- `--delete` delete documents one-by-one from the input as they are moved (default: false)
- `--all` load/store documents from ALL indices (default: false)
- `--bulk` leverage elasticsearch Bulk API when writing documents (default: false)
- `--ignore-errors` will continue the read/write loop on write error (default: false)
- `--scrollTime` Time the nodes will hold the requested search in order. (default: 10m)
- `--maxSockets` How many simultanius HTTP requests can this process make? (default: 5 [node <= v0.10.x] / Infinity [node >= v0.11.x] )

## Elasticsearch's scan and scroll method
Elasticsearch provides a scan and scroll method to fetch all documents of an index. This method is much safer to use since
it will maintain the result set in cache for the given period of time. This means it will be a lot faster to export the data
and more important it will keep the result set in order. While dumping the result set in batches it won't export duplicate
documents in the export. All documents in the export will unique and therefore no missing documents.

NOTE: only works for output

## Notes

- elasticdump (and elasticsearch in general) will create indices if they don't exist upon import
- we are using the `put` method to write objects.  This means new objects will be created and old objects with the same ID will be updated
- the `file` transport will overwrite any existing files
- If you need basic http auth, you can use it like this: `--input=http://name:password@production.es.com:9200/my_index`
- if you choose a stdio output (`--output=$`), you can also request a more human-readable output with `--format=human`
- if you choose a stdio output (`--output=$`), all logging output will be suppressed

Inspired by https://github.com/crate/elasticsearch-inout-plugin and https://github.com/jprante/elasticsearch-knapsack

Built at [TaskRabbit](https://www.taskrabbit.com)
