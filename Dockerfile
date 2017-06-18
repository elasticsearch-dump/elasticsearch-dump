FROM alpine:3.3
MAINTAINER korolkov@gmail.com

RUN apk add --update nodejs

RUN npm install https://github.com/azbix/elasticsearch-dump -g

ENTRYPOINT ["/usr/lib/node_modules/elasticdump/bin/elasticdump"]
