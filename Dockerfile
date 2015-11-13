FROM ubuntu:trusty
MAINTAINER arthur@caranta.com

RUN apt-get update -y
RUN apt-get install -y npm
RUN npm install elasticdump -g
#Fix nodejs path (thx debian/ubuntu package ...)
RUN ln -s "$(which nodejs)" /usr/bin/node

#And finally use elasticdump as the entrypoint
ENTRYPOINT ["/usr/local/bin/elasticdump"]
