FROM ubuntu:trusty
MAINTAINER evan@evantahler.com

RUN apt-get update -y && \
    apt-get install -y curl && \
    curl -sL https://deb.nodesource.com/setup_4.x | sudo -E bash - && \
    apt-get install -y nodejs && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN npm install elasticdump -g

#And finally use elasticdump as the entrypoint
ENTRYPOINT ["/usr/lib/node_modules/elasticdump/bin/elasticdump"]
