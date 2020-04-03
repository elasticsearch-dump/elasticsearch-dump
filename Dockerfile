FROM node:12-alpine
LABEL maintainer="ferronrsmith@gmail.com"

ENV NODE_ENV production

RUN npm install elasticdump -g

COPY docker-entrypoint.sh /usr/local/bin/

ENTRYPOINT ["docker-entrypoint.sh"]

CMD ["elasticdump"]
