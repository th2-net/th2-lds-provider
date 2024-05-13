plugins {
    `java-library`
    `maven-publish`
}

val commonVersion: String by rootProject.extra

dependencies {
    api(project(":grpc-lw-data-provider"))
    implementation("com.exactpro.th2:common:${commonVersion}") {
        exclude(group = "com.exactpro.th2", module = "cradle-core")
        exclude(group = "com.exactpro.th2", module = "cradle-cassandra")
    }

    implementation("io.github.microutils:kotlin-logging:3.0.5")

    testImplementation("org.mockito.kotlin:mockito-kotlin:5.3.1")
    testImplementation("org.jetbrains.kotlin:kotlin-test-junit5")
}