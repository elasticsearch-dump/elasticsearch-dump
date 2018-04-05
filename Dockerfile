FROM node:8.11-alpine
LABEL maintainer="evan@evantahler.com"

RUN npm install elasticdump -g

ENTRYPOINT ["elasticdump"]
