import com.google.protobuf.gradle.*
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    id("com.google.protobuf")
    // We need this to add generate source file to intellij
    id("idea")
    kotlin("jvm")
}

dependencies {
    api(platform(project(":bom")))

    api("io.grpc:grpc-netty-shaded")
    api("io.grpc:grpc-protobuf")
    api("io.grpc:grpc-stub")
    api("javax.annotation:javax.annotation-api")
    api("io.grpc:grpc-kotlin-stub")
    api("org.jetbrains.kotlinx:kotlinx-coroutines-core")
    implementation(kotlin("stdlib-jdk8"))
}

sourceSets {
    main {
        proto {
            srcDir("${rootProject.rootDir}/../proto")
        }
    }
}

protobuf {
    protoc {
        // Since `protoc` does not provide pre-built binary for arm64 architecture, we use this
        // trick to force download x86_64 binary on apple silicon machine.
        if (osdetector.os == "osx") {
            artifact = "com.google.protobuf:protoc:3.0.0:osx-x86_64"
        } else {
            artifact = "com.google.protobuf:protoc:3.0.0"
        }
    }

    plugins {
        // Optional: an artifact spec for a protoc plugin, with "grpc" as
        // the identifier, which can be referred to in the "plugins"
        // container of the "generateProtoTasks" closure.
        id("grpc") {
            // Ditto. This trick is for aplle silicon users.
            if (osdetector.os == "osx") {
                artifact = "io.grpc:protoc-gen-grpc-java:1.15.1:osx-x86_64"
            } else {
                artifact = "io.grpc:protoc-gen-grpc-java:1.15.1"
            }
        }
        id("grpckt") {
            artifact = "io.grpc:protoc-gen-grpc-kotlin:1.1.0:jdk7@jar"
        }
    }
    generateProtoTasks {
        ofSourceSet("main").forEach {
            // See https://github.com/google/protobuf-gradle-plugin/issues/331#issuecomment-543333726
            // Old proto generated files being undeleted causes build failure.
            it.doFirst {
                delete(this.outputs)
            }
            it.plugins {
                // Apply the "grpc" plugin whose spec is defined above, without options.
                id("grpc")
                id("grpckt")
            }
        }
    }
}
repositories {
    mavenCentral()
}
val compileKotlin: KotlinCompile by tasks
compileKotlin.kotlinOptions {
    jvmTarget = "11"
}
val compileTestKotlin: KotlinCompile by tasks
compileTestKotlin.kotlinOptions {
    jvmTarget = "11"
}
