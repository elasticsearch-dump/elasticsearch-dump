FROM node:8.12-alpine
LABEL maintainer="evan@evantahler.com"

RUN npm install elasticdump -g

ENTRYPOINT ["elasticdump"]
