FROM node:10-alpine
LABEL maintainer="evan@evantahler.com"

RUN npm install elasticdump -g

ENTRYPOINT ["elasticdump"]
