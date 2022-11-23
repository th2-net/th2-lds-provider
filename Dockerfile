FROM gradle:7.1-jdk11 AS build
COPY ./ .
RUN gradle build dockerPrepare -Prelease_version=${Prelease_version}

FROM adoptopenjdk/openjdk11:alpine
WORKDIR /home
COPY --from=build /home/gradle/build/docker .
ENTRYPOINT ["/home/service/bin/service", "run", "com.exactpro.th2.lwdataprovider.MainKt"]
