#!/bin/bash
#
# ES_VERSION=${1}
ES_FLAGS=
echo "running es-version ${ES_VERSION}"
if [ "${ES_VERSION}" == "7.0.0" ]; then
	ES_DOWNLOAD_URL="https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-${ES_VERSION}-linux-x86_64.tar.gz"
	ES_FLAGS="-Ediscovery.type=single-node"
else
	ES_DOWNLOAD_URL="https://download.elastic.co/elasticsearch/elasticsearch/elasticsearch-${ES_VERSION}.tar.gz"
fi


echo "url ${ES_DOWNLOAD_URL}"

## ES has different download locations for each version, so we'll download them both and then just use the one we want
curl -Lo elasticsearch.tar.gz "${ES_DOWNLOAD_URL}"

## Now, use the ENV to choose the version
tar -xzf elasticsearch.tar.gz

export ES_JAVA_OPTS="-Xms512m -Xmx512m"
./elasticsearch-${ES_VERSION}/bin/elasticsearch ${ES_FLAGS} &

# ES needs some time to start
wget -q --waitretry=1 --retry-connrefused -T 240 -O - http://127.0.0.1:9200

npm test