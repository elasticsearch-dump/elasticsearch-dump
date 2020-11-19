FROM node:14-buster-slim
LABEL maintainer="ferronrsmith@gmail.com"
ARG ES_DUMP_VER
ENV ES_DUMP_VER=${ES_DUMP_VER:-latest}
ENV NODE_ENV production

RUN npm install elasticdump@${ES_DUMP_VER} -g

COPY docker-entrypoint.sh /usr/local/bin/

ENTRYPOINT ["docker-entrypoint.sh"]

CMD ["elasticdump"]
