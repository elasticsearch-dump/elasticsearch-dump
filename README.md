elasticdump
==================

Tools for moving and saving indicies.

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
- format:  `{proticol}://{host}:{port}/{index}`
- example: `http://127.0.0.1:9200/my_index`

File:
- format:  `{FilePath}`
- example: `/Users/evantahler/Desktop/dump.json`

You can then do things like:

- Copy an index from production to staging: 
  - `elasticdump --input=http://production.es.com:9200/my_index --output=http://staging.es.com:9200/my_index`
- Backup an index to a file: 
  - `elasticdump --input=http://production.es.com:9200/my_index --output=/var/dat/es.json`
- Backup ALL indices, then use Bulk API to load populate another ES cluster:
  - `elasticdump --all=true --input=http://staging.es.com:9200/ --output=/var/dat/es.json`
  - `elasticdump --all=true --input=/var/dat/es.json --output=http://production.es.com:9200/ --bulk=true`

## Options

- `--input` (required) (see above)
- `--output` (required) (see above)
- `--limit` how many ojbects to move in bulk per operation (default: 100)
- `--debug` display the elasticsearch commands being used (default: false)
- `--delete` delete documents one-by-one from the input as they are moved (default: false)
- `--all` load/store documents from ALL indices (default: false)
- `--bulk` leverage elasticsearch Bulk API when writing documents (default: false)

## Notes

- elasticdump (and elasticsearch in general) will create indices if they don't exist upon import
- we are using the `put` method to write objects.  This means new objects will be created and old objects with the same ID will be updated
- the `file` transport will overwrite any existing files
- If you need basic http auth, you can use it like this: `--input=http://name:password@production.es.com:9200/my_index`

Inspired by https://github.com/crate/elasticsearch-inout-plugin and https://github.com/jprante/elasticsearch-knapsack

Built at [TaskRabbit](https://www.taskrabbit.com)
