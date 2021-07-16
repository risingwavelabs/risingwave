plugins {
    java
}

subprojects {
    group = "com.risingwave"
    version = "0.0.1-SNAPSHOT"

    repositories {
        mavenCentral()
    }

    // bom is java-platform, can't apply java-library plugin
    if (name != "bom") {
        apply(plugin = "java-library")
    }

    apply<CheckstylePlugin>()
    configure<CheckstyleExtension> {
        val configLoc = File(rootDir, "src/main/resources/checkstyle")
        configDirectory.set(configLoc)
        isShowViolations = true
        toolVersion = "8.44"
        maxWarnings = 0
    }
}

