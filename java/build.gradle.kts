buildscript {
    repositories {
        maven {
            url = uri("https://plugins.gradle.org/m2/")
        }
    }
    dependencies {
        classpath("com.diffplug.spotless:spotless-plugin-gradle:5.14.1")
    }
}

plugins {
    java
}

if (JavaVersion.current() != JavaVersion.VERSION_1_8) {
    throw GradleException("Only java 8 is supported!")
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

        java {
            sourceCompatibility = JavaVersion.VERSION_1_8
            targetCompatibility = JavaVersion.VERSION_1_8
        }

        apply<com.diffplug.gradle.spotless.SpotlessPlugin>()
        configure<com.diffplug.gradle.spotless.SpotlessExtension> {
            ratchetFrom = "origin/master"
            java {
                importOrder() // standard import order
                removeUnusedImports()
                googleJavaFormat()
            }
        }


        tasks.named<Test>("test") {
            useJUnitPlatform()
        }
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
