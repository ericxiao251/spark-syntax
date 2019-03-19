FROM ubuntu:16.04

ENV LANG C.UTF-8

ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    apt-utils software-properties-common nodejs npm git make sudo

RUN ln -s /usr/bin/nodejs /usr/bin/node

# ignore gpg key exit status
RUN add-apt-repository -y ppa:jonathonf/calibre; exit 0

# install calibre v3.29
RUN apt-get update && apt-get install -y calibre

RUN npm install -g gitbook-cli@2.3.0

# Replace 1000 with your user / group id
RUN export uid=1000 gid=1000 && \
    mkdir -p /app/gitbook && \
    echo "docker:x:${uid}:${gid}:Docker,,,:/app:/bin/bash" >> /etc/passwd && \
    echo "docker:x:${uid}:" >> /etc/group && \
    echo "docker ALL=(ALL) NOPASSWD: ALL" > /etc/sudoers.d/docker && \
    chmod 0440 /etc/sudoers.d/docker && \
    chown ${uid}:${gid} -R /app

USER docker

RUN gitbook fetch 3.2.x

WORKDIR /app/gitbook

EXPOSE 4000
