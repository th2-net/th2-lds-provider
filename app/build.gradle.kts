plugins {
    `java-library`
    `maven-publish`

    alias(libs.plugins.kapt)
    alias(libs.plugins.serialization)
    alias(libs.plugins.th2.component)
}

dependencies {
    implementation(libs.th2.common) {
        exclude(group = "com.exactpro.th2", module = "cradle-core")
        exclude(group = "com.exactpro.th2", module = "cradle-cassandra")
    }

    implementation(platform(libs.jetty.bom))

    implementation(libs.javalin)
    implementation(libs.javalin.micrometer)

    implementation(platform(libs.micrometer.bom)) {
        because("should match the version in javalin-micrometer")
    }

    implementation("io.micrometer:micrometer-registry-prometheus")
    implementation("org.apache.commons:commons-lang3")

    kapt(libs.openapi.annotation.processor)

    implementation(libs.javalin.openapi.plugin) {
        because("for /openapi route with JSON scheme")
    }
// swagger-ui-3.52.5.jar (pkg:maven/org.webjars/swagger-ui@3.52.5) : CVE-2018-25031
//    implementation("io.javalin.community.openapi:javalin-swagger-plugin:$javalin") {
//        because("for Swagger UI")
//    }
    implementation(libs.javalin.redoc.plugin) {
        because("for Re Doc UI")
    }

    implementation(libs.cradle.cassandra)
    implementation(libs.lz4) {
        because("cassandra driver requires lz4 impl in classpath for compression")
    }
    implementation(project(":grpc-lw-data-provider"))

    implementation("io.prometheus:simpleclient") {
        because("need add custom metrics to provider")
    }

    implementation("org.apache.commons:commons-lang3")

    implementation("com.fasterxml.jackson.core:jackson-core")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310")

    implementation(libs.kotlinx.serialization.json.jvm)

    implementation("io.netty:netty-buffer")

    testImplementation(libs.javalin.testtools)

    testImplementation(testFixtures(libs.th2.common))
    testImplementation(platform(libs.testcontainers.bom))
    testImplementation("org.testcontainers:junit-jupiter")
    testImplementation("org.testcontainers:cassandra")

    testImplementation("com.datastax.oss:java-driver-core")
}

application {
    mainClass.set("com.exactpro.th2.lwdataprovider.MainKt")
}