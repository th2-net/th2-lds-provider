plugins {
    `java-library`
    `maven-publish`

    kotlin("kapt")
    kotlin("plugin.serialization")
    id("com.exactpro.th2.gradle.component")
}

val cradleApiVersion: String by rootProject.extra
val javalin: String by rootProject.extra
val commonVersion: String by rootProject.extra

dependencies {
    implementation("com.exactpro.th2:common:${commonVersion}") {
        exclude(group = "com.exactpro.th2", module = "cradle-core")
        exclude(group = "com.exactpro.th2", module = "cradle-cassandra")
    }

    implementation(platform("org.eclipse.jetty:jetty-bom:11.0.20")) {
        because("vulnerabilities in version 11.0.13. Can be removed after updating Javalin and Kotlin")
    }

    implementation("io.javalin:javalin:$javalin")
    implementation("io.javalin:javalin-micrometer:$javalin")

    implementation(platform("io.micrometer:micrometer-bom:1.12.5")) {
        because("should match the version in javalin-micrometer")
    }

    implementation("io.micrometer:micrometer-registry-prometheus")
    implementation("org.apache.commons:commons-lang3")

    kapt("io.javalin.community.openapi:openapi-annotation-processor:$javalin")

    implementation("io.javalin.community.openapi:javalin-openapi-plugin:$javalin") {
        because("for /openapi route with JSON scheme")
    }
// swagger-ui-3.52.5.jar (pkg:maven/org.webjars/swagger-ui@3.52.5) : CVE-2018-25031
//    implementation("io.javalin.community.openapi:javalin-swagger-plugin:$javalin") {
//        because("for Swagger UI")
//    }
    implementation("io.javalin.community.openapi:javalin-redoc-plugin:$javalin") {
        because("for Re Doc UI")
    }

    implementation("com.exactpro.th2:cradle-cassandra:$cradleApiVersion")
    implementation("net.jpountz.lz4:lz4:1.3.0") {
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

    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json-jvm:1.5.1")

    implementation("io.netty:netty-buffer")

    testImplementation("io.javalin:javalin-testtools:$javalin")

    testImplementation(testFixtures("com.exactpro.th2:common:${commonVersion}"))
    testImplementation(platform("org.testcontainers:testcontainers-bom:1.19.8"))
    testImplementation("org.testcontainers:junit-jupiter")
    testImplementation("org.testcontainers:cassandra")

    testImplementation("com.datastax.oss:java-driver-core")
}

application {
    mainClass.set("com.exactpro.th2.lwdataprovider.MainKt")
}