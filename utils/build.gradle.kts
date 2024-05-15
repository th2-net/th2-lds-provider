plugins {
    `java-library`
    `maven-publish`
}

dependencies {
    api(project(":grpc-lw-data-provider"))
    implementation(libs.th2.common) {
        exclude(group = "com.exactpro.th2", module = "cradle-core")
        exclude(group = "com.exactpro.th2", module = "cradle-cassandra")
    }

    implementation(libs.kotlin.logging)

    testImplementation(libs.mockito.kotlin)
    testImplementation("org.jetbrains.kotlin:kotlin-test-junit5")
}