FROM azul/zulu-openjdk-alpine:21-latest
WORKDIR /home
COPY ./app/build/docker .
ENTRYPOINT ["/home/service/bin/service"]
