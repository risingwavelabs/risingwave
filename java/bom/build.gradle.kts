plugins {
    `java-platform`
}

val junit5Version = "5.7.2"
val calciteVersion = "1.27.0"

dependencies {
    constraints {
        api("org.apache.calcite:calcite-core:$calciteVersion")
        // For calcite ddl parser, remove it when our parser is ready
        api("org.apache.calcite:calcite-server:$calciteVersion")
        api("com.google.guava:guava:30.1.1-jre")
        api("org.slf4j:slf4j-api:2.0.0-alpha2")
        api("org.junit.jupiter:junit-jupiter-api:$junit5Version")
        api("io.ktor:ktor-network:1.6.1")
        api("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.5.1")
        api("io.netty:netty-buffer:4.1.66.Final")
        api("org.duckdb:duckdb_jdbc:0.2.7")
        api("org.reflections:reflections:0.9.12")
        runtime("ch.qos.logback:logback-classic:1.2.3")
        runtime("org.junit.jupiter:junit-jupiter-engine:$junit5Version")
        runtime("org.junit.jupiter:junit-jupiter-params:$junit5Version")
    }
}
