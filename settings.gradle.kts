dependencyResolutionManagement {
    repositoriesMode.set(RepositoriesMode.FAIL_ON_PROJECT_REPOS)
    repositories {
        mavenCentral()
        maven {
            name = "Sonatype_snapshots"
            url = uri("https://s01.oss.sonatype.org/content/repositories/snapshots/")
        }

        maven {
            name = "Sonatype_releases"
            url = uri("https://s01.oss.sonatype.org/content/repositories/releases/")
        }
    }
}

include("app")
include("grpc")
include("utils")

project(":app").name = "lw-data-provider"
project(":grpc").name = "grpc-lw-data-provider"
project(":utils").name = "lw-data-provider-utils"