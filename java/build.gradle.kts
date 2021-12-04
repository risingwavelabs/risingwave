buildscript {
    repositories {
        gradlePluginPortal()
    }
    dependencies {
        classpath("com.diffplug.spotless:spotless-plugin-gradle:5.17.1")
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

apply(plugin = "git")
// Absolute paths of all changed files
val changedFiles: List<String> by extra
println("Changed files: $changedFiles")

apply(plugin = "jacoco")
tasks.register<JacocoReport>("jacocoRootReport") {
    reports {
        xml.required.set(true)
        html.required.set(true)
    }
    subprojects {
        this@subprojects.plugins.withType<JacocoPlugin>().configureEach {
            this@subprojects.tasks.matching {
                !listOf(
                    "catalog:test",
                    "risingwave:test",
                    "meta:test",
                    "proto:test",
                    "pgserver:test",
                    "pgserver:run",
                ).contains("${it.project.name}:${it.name}")
            }.configureEach {
                sourceSets(this@subprojects.the<SourceSetContainer>().named("main").get())
                executionData(this)
            }
        }
    }
}

apply(plugin = "jacoco")
tasks.register<JacocoReport>("jacocoE2eReport") {
    reports {
        xml.required.set(true)
        html.required.set(true)
    }
    subprojects {
        this@subprojects.plugins.withType<JacocoPlugin>().configureEach {
            this@subprojects.tasks.matching {
                !listOf(
                    "catalog:test",
                    "risingwave:test",
                    "meta:test",
                    "proto:test",
                    "pgserver:test"
                ).contains("${it.project.name}:${it.name}")
            }.configureEach {
                sourceSets(this@subprojects.the<SourceSetContainer>().named("main").get())
                executionData(this)
            }
        }
    }
}

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
            ratchetFrom = "origin/main"
            java {
                importOrder() // standard import order
                removeUnusedImports()
                googleJavaFormat()

                targetExclude(
                    "src/main/java/com/risingwave/sql/SqlFormatter.java",
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
            reports {
                xml.required.set(true)
                html.required.set(true)
            }
        }
        tasks.withType<JacocoReport> {
            afterEvaluate {
                classDirectories.setFrom(
                    files(
                        classDirectories.files.map {
                            fileTree(it).apply {
                                exclude("**/antlr/**")
                            }
                        }
                    )
                )
            }
        }
    }

    apply(plugin = "checkstyle")
    configure<CheckstyleExtension> {
        val configLoc = File(rootDir, "codestyle")
        configDirectory.set(configLoc)
        isShowViolations = true
        toolVersion = "8.44"
        maxWarnings = 0
    }

    tasks.withType<Checkstyle> {
        val allSrcDirs = project.sourceSets
            .flatMap { it.allSource.srcDirs }
            .map { it.absolutePath }
            .toList()

        // We need this line because if nothing changed in this project, checkstyle task just includes all
        // source files, which is what we want to avoid.
        include("")
        for (changedFile in changedFiles) {
            for (srcDir in allSrcDirs) {
                if (changedFile.startsWith(srcDir)) {
                    include(changedFile.substring(srcDir.length + 1))
                    break
                }
            }
        }
    }
}