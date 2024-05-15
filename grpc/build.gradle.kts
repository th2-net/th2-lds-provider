plugins {
    `java-library`
    `maven-publish`

    alias(libs.plugins.th2.grpc)
}

dependencies {
    api(libs.th2.grpc.common)
}

th2Grpc {
    service.set(true)
}