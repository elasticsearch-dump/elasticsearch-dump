FROM node:8.9-alpine
LABEL maintainer="evan@evantahler.com"

RUN npm install elasticdump -g

ENTRYPOINT ["/usr/lib/node_modules/elasticdump/bin/elasticdump"]
