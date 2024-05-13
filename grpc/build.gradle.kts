plugins {
    `java-library`
    `maven-publish`

    id("com.exactpro.th2.gradle.grpc")
}

dependencies {
    api("com.exactpro.th2:grpc-common:4.5.0-dev")
}

th2Grpc {
    service.set(true)
}