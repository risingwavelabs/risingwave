plugins {
    `java-platform`
}

val junit5Version = "5.7.2"
val calciteVersion = "1.27.0"
val grpcVersion = "1.43.2"
val annotationVersion = "1.3.2"
var guiceVersion = "5.0.1"
val protobufVersion = "3.18.2"
val akkaVersion = "2.6.15"
val scalaBinaryVersion = "2.13"
val mockitoVersion = "3.11.2"
var hamcrestVersion = "1.3"
var quickcheckVersion = "1.0"
val logbackVersion = "1.2.10"

javaPlatform {
    allowDependencies()
}

dependencies {
    api(platform("com.typesafe.akka:akka-bom_${scalaBinaryVersion}:${akkaVersion}"))

    constraints {
        api("org.apache.calcite:calcite-core:$calciteVersion")
        // For calcite ddl parser, remove it when our parser is ready
        api("org.apache.calcite:calcite-server:$calciteVersion")
        api("com.google.guava:guava:30.1.1-jre")
        api("org.slf4j:slf4j-api:1.7.9")
        api("org.junit.jupiter:junit-jupiter-api:$junit5Version")
        api("io.ktor:ktor-network:1.6.1")
        api("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.5.1")
        api("io.netty:netty-buffer:4.1.66.Final")
        api("org.duckdb:duckdb_jdbc:0.2.7")
        api("org.reflections:reflections:0.9.12")
        api("com.github.pcj:google-options:1.0.0")
        api("io.grpc:grpc-netty-shaded:${grpcVersion}")
        api("io.grpc:grpc-protobuf:${grpcVersion}")
        api("io.grpc:grpc-stub:${grpcVersion}")
        api("com.google.inject:guice:$guiceVersion")
        api("javax.annotation:javax.annotation-api:$annotationVersion")
        api("com.google.protobuf:protobuf-java-util:${protobufVersion}")
        api("org.mockito:mockito-core:${mockitoVersion}")
        api("com.pholser:junit-quickcheck-core:$quickcheckVersion")
        api("org.hamcrest:hamcrest-all:$hamcrestVersion")
        api("org.apache.commons:commons-lang3:3.0")
        api("io.grpc:grpc-kotlin-stub:1.1.0")
        runtime("ch.qos.logback:logback-classic:${logbackVersion}")
        runtime("org.junit.jupiter:junit-jupiter-engine:$junit5Version")
        runtime("org.junit.jupiter:junit-jupiter-params:$junit5Version")
    }
}
