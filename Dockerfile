FROM node:8.9-alpine
LABEL maintainer="ferronrsmith@gmail.com"

RUN npm install elasticdump -g

ENTRYPOINT ["/usr/lib/node_modules/elasticdump/bin/elasticdump"]
