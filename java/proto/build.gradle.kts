import com.google.protobuf.gradle.*

plugins {
    id("com.google.protobuf")
    // We need this to add generate source file to intellij
    id("idea")
}

dependencies {
    api(platform(project(":bom")))

    api("io.grpc:grpc-netty-shaded")
    api("io.grpc:grpc-protobuf")
    api("io.grpc:grpc-stub")
    api("javax.annotation:javax.annotation-api")
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
        artifact = "com.google.protobuf:protoc:3.0.0"
    }

    plugins {
        // Optional: an artifact spec for a protoc plugin, with "grpc" as
        // the identifier, which can be referred to in the "plugins"
        // container of the "generateProtoTasks" closure.
        id("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:1.15.1"
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
            }
        }
    }
}