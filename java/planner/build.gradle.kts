plugins {
    antlr
}

// TODO: We need to figure out one way to manage all version in one place
val scalaBinaryVersion = "2.13"

dependencies {
    // Get recommended versions from platform project
    api(platform(project(":bom")))

    // Declare dependencies, no version required
    api("org.apache.calcite:calcite-core")
    api(project(":common"))
    api(project(":catalog"))
    api(project(":pgwire"))
    implementation(project(":proto"))
    api("org.slf4j:slf4j-api")
    api("org.reflections:reflections")
    api("com.typesafe.akka:akka-actor-typed_${scalaBinaryVersion}")
    api("com.google.inject:guice")
    runtimeOnly("ch.qos.logback:logback-classic")

    testImplementation("org.junit.jupiter:junit-jupiter-api")
    testImplementation("org.junit.jupiter:junit-jupiter-params")
    testImplementation("org.apache.calcite:calcite-server")
    testImplementation("com.google.protobuf:protobuf-java-util")
    testImplementation("org.mockito:mockito-core")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
    testImplementation("com.pholser:junit-quickcheck-core")
    testImplementation("org.hamcrest:hamcrest-all")
    // TODO: Manage all dependency versions in one place.
    antlr("org.antlr:antlr4:4.9.2")
}

tasks.generateGrammarSource {
    arguments = arguments + listOf("-visitor")
    outputDirectory = File("build/generated-src/antlr/main/com/risingwave/parser/antlr/v4")
}