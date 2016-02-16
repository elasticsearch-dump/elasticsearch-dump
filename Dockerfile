FROM ubuntu:trusty
MAINTAINER evan@evantahler.com

RUN apt-get update -y
RUN curl -sL https://deb.nodesource.com/setup_4.x | sudo -E bash -
RUN apt-get install -y nodejs && apt-get clean && rm -rf /var/lib/apt/lists/*

#Fix nodejs path (thx debian/ubuntu package ...)
RUN ln -s "$(which nodejs)" /usr/bin/node

RUN npm install elasticdump -g

#And finally use elasticdump as the entrypoint
ENTRYPOINT ["/usr/local/bin/elasticdump"]
