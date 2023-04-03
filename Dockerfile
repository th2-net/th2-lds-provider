#FROM adoptopenjdk/openjdk11:alpine

# FIXME: remove when release
FROM adoptopenjdk/openjdk11:x86_64-ubuntu-jdk-11.0.11_9
RUN cat /etc/os-release
RUN apt-get update
RUN apt-get install -y ifstat
RUN apt-get install -y curl

WORKDIR /home
COPY ./build/docker .
ENTRYPOINT ["/home/service/bin/service"]
