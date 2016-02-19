FROM alpine:3.3
MAINTAINER evan@evantahler.com

RUN apk add --update nodejs

RUN npm install elasticdump -g

ENTRYPOINT ["/usr/lib/node_modules/elasticdump/bin/elasticdump"]
