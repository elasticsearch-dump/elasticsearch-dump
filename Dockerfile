FROM node:20-bookworm-slim
LABEL maintainer="ferronrsmith@gmail.com"
ARG ES_DUMP_VER
ARG TARGETPLATFORM
ENV ES_DUMP_VER=${ES_DUMP_VER:-latest}
ENV NODE_ENV production

RUN apt-get -y update && \
    apt-get -y install wget &&  \
    if [ "$TARGETPLATFORM" = "linux/amd64" ]; then ARCHITECTURE=amd64; elif [ "$TARGETPLATFORM" = "linux/arm64" ]; then ARCHITECTURE=arm64; else ARCHITECTURE=amd64; fi && \
    wget "https://github.com/Yelp/dumb-init/releases/download/v1.2.5/dumb-init_1.2.5_${ARCHITECTURE}.deb" && \
    dpkg -i dumb-init_*.deb

RUN npm install elasticdump@${ES_DUMP_VER} -g

COPY docker-entrypoint.sh /usr/local/bin/

ENTRYPOINT ["docker-entrypoint.sh"]

CMD ["/usr/bin/dumb-init", "elasticdump"]
