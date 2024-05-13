plugins {
    id("com.exactpro.th2.gradle.base") version "0.0.6"
    id("com.exactpro.th2.gradle.publish") version "0.0.6"

    kotlin("jvm") version "1.8.22"
    kotlin("kapt") version "1.8.22" apply false
    id("org.jetbrains.kotlin.plugin.serialization") version "1.8.22" apply false

    id("com.exactpro.th2.gradle.grpc") version "0.0.6" apply false
    id("com.exactpro.th2.gradle.component") version "0.0.6" apply false
}

val cradleApiVersion by extra("5.3.0-dev")
val javalin by extra("5.4.2")
val commonVersion by extra("5.11.0-dev")

dependencyCheck {
    skipConfigurations = listOf("kapt", "kaptClasspath_kaptKotlin", "kaptTest", "kaptTestFixtures")
    suppressionFile = "suppressions.xml"
}

allprojects {
    group = "com.exactpro.th2"
    version = project.findProperty("release_version")
    val suffix: String = project.findProperty("version_suffix").toString()
    if (!suffix.isEmpty()) {
        version = "$version-$suffix"
    }
}

subprojects {
    apply(plugin = "kotlin")

    kotlin {
        jvmToolchain(11)
    }

    configurations.all {
        resolutionStrategy.cacheChangingModulesFor(0, "seconds")
        resolutionStrategy.cacheDynamicVersionsFor(0, "seconds")
    }

    dependencies {
        implementation("org.slf4j:slf4j-api")
        implementation("io.github.microutils:kotlin-logging:3.0.5")

        testImplementation("org.apache.logging.log4j:log4j-slf4j2-impl") {
            because("logging in testing")
        }
        testImplementation("org.apache.logging.log4j:log4j-core") {
            because("logging in testing")
        }
        testImplementation("org.junit.jupiter:junit-jupiter:5.10.2")
        testImplementation("org.mockito.kotlin:mockito-kotlin:5.3.1")
        testImplementation("io.strikt:strikt-core:0.34.1")
        testImplementation("io.strikt:strikt-jackson:0.34.1")
    }

    tasks.test {
        useJUnitPlatform {
            excludeTags("integration-test")
        }
    }

    tasks.register<Test>("integrationTest") {
        group = "verification"
        useJUnitPlatform {
            includeTags("integration-test")
        }
        testLogging {
            showStandardStreams = true
        }
    }
}