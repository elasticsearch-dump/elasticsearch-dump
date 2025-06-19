FROM node:20.18.3-bookworm-slim
LABEL maintainer="ferronrsmith@gmail.com"
ARG ES_DUMP_VER
ARG TARGETPLATFORM
ENV ES_DUMP_VER=${ES_DUMP_VER:-latest}
ENV NODE_ENV=production

RUN npm install elasticdump@${ES_DUMP_VER} -g

COPY docker-entrypoint.sh /usr/local/bin/

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]

CMD ["elasticdump"]