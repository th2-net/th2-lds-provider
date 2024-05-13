dependencyResolutionManagement {
    repositoriesMode.set(RepositoriesMode.FAIL_ON_PROJECT_REPOS)
    repositories {
        mavenCentral()
    }
}

include("app")
include("grpc")
include("utils")

project(":app").name = "lw-data-provider"
project(":grpc").name = "grpc-lw-data-provider"
project(":utils").name = "lw-data-provider-utils"