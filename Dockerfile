FROM node:10-alpine
LABEL maintainer="evan@evantahler.com"

ENV NODE_ENV production

RUN npm install elasticdump -g

COPY docker-entrypoint.sh /usr/local/bin/

ENTRYPOINT ["docker-entrypoint.sh"]

CMD ["elasticdump"]
