plugins {
    alias(libs.plugins.th2.base)
    alias(libs.plugins.th2.publish)

    alias(libs.plugins.kotlin)
    alias(libs.plugins.kapt) apply false
    alias(libs.plugins.serialization) apply false

    alias(libs.plugins.th2.grpc) apply false
    alias(libs.plugins.th2.component) apply false
}

dependencyCheck {
    skipConfigurations = listOf("kapt", "kaptClasspath_kaptKotlin", "kaptTest", "kaptTestFixtures")
    suppressionFile = "suppressions.xml"
}

allprojects {
    group = "com.exactpro.th2"
    version = project.findProperty("release_version").toString()
    val suffix: String = project.findProperty("version_suffix").toString()
    if (suffix.isNotEmpty()) {
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
        implementation(rootProject.libs.kotlin.logging)

        testImplementation("org.apache.logging.log4j:log4j-slf4j2-impl") {
            because("logging in testing")
        }
        testImplementation("org.apache.logging.log4j:log4j-core") {
            because("logging in testing")
        }
        implementation(rootProject.libs.junit.jupiter)
        implementation(rootProject.libs.mockito.kotlin)
        implementation(rootProject.libs.strikt.core)
        implementation(rootProject.libs.strikt.jackson)
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