FROM adoptopenjdk/openjdk11:alpine
RUN cat /etc/os-release
RUN apt-get update
RUN apt-get install -y ifstat
RUN apt-get install -y curl

WORKDIR /home
COPY ./build/docker .
ENTRYPOINT ["/home/service/bin/service"]
