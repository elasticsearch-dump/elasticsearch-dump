FROM alpine:3.6
LABEL maintainer="evan@evantahler.com"

RUN apk add --update nodejs nodejs-npm && \
    npm install elasticdump -g && \
    rm -rf /var/cache/apk/*

ENTRYPOINT ["/usr/lib/node_modules/elasticdump/bin/elasticdump"]
