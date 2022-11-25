FROM adoptopenjdk/openjdk11:alpine
WORKDIR /home
COPY ./build/docker .
ENTRYPOINT ["/home/service/bin/service"]
