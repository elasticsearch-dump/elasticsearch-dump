elasticdump
==================

Tools for moving and saving indicies.

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

## Options

- `--input` (required) (see above)
- `--output` (required) (see above)
- `--limit` how many ojbects to move in bulk per operation
- `--debug` display the elasticsearch commands being used

## Notes

- elasticdump (and elasticsearch in general) will create indices if they don't exist upon import
- we are using the `put` method to write objects.  This means new objects will be created and old objects with the same ID will be updated
- the `file` trnasport will overwrite any existing files

Inspired by https://github.com/crate/elasticsearch-inout-plugin and https://github.com/jprante/elasticsearch-knapsack