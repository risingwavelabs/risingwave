buildscript {
    repositories {
        gradlePluginPortal()
    }
    dependencies {
        classpath("com.diffplug.spotless:spotless-plugin-gradle:5.14.1")
        classpath("com.google.protobuf:protobuf-gradle-plugin:0.8.17")
    }
}

plugins {
    java
    checkstyle
    jacoco
    id("com.diffplug.spotless").version("5.14.1")
}

repositories {
    // Required to download KtLint
    mavenCentral()
}

val javaVersion = JavaVersion.VERSION_11
if (JavaVersion.current() != javaVersion) {
    throw GradleException("Only $javaVersion is supported!")
}

val bomProject = "bom"
val appProjects = setOf("pgserver")


subprojects {
    group = "com.risingwave"
    version = "0.0.1-SNAPSHOT"

    repositories {
        mavenCentral()
    }

    // bom is java-platform, can't apply java-library plugin
    if (name != bomProject) {
        if (appProjects.contains(name)) {
            apply(plugin = "application")
        } else {
            apply(plugin = "java-library")
        }

        java {
            sourceCompatibility = javaVersion
            targetCompatibility = javaVersion
        }

        apply<com.diffplug.gradle.spotless.SpotlessPlugin>()
        configure<com.diffplug.gradle.spotless.SpotlessExtension> {
            ratchetFrom = "origin/master"
            java {
                importOrder() // standard import order
                removeUnusedImports()
                googleJavaFormat()

                targetExclude("src/main/java/com/risingwave/sql/SqlFormatter.java",
                    "src/main/java/org/apache/calcite/**"
                )
            }
            kotlin {
                ktlint("0.37.2").userData(mapOf("indent_size" to "2"))
            }
        }


        tasks.test {
            useJUnitPlatform()
        }

        apply(plugin = "jacoco")
        tasks.jacocoTestReport {
            dependsOn(tasks.test) // tests are required to run before generating the report
            reports {
                xml.required.set(true)
                csv.required.set(false)
                html.required.set(true)
            }
        }

        tasks.withType<JacocoReport> {
            afterEvaluate {
                classDirectories.setFrom(files(classDirectories.files.map {
                    fileTree(it).apply {
                        exclude("**/antlr/**")
                    }
                }))
            }
        }
    }

    apply<CheckstylePlugin>()
    configure<CheckstyleExtension> {
        val configLoc = File(rootDir, "codestyle")
        configDirectory.set(configLoc)
        isShowViolations = true
        toolVersion = "8.44"
        maxWarnings = 0
    }
}
