plugins {
    antlr
}

dependencies {
    // Get recommended versions from platform project
    api(platform(project(":bom")))

    // Declare dependencies, no version required
    api("org.apache.calcite:calcite-core")
    api(project(":common"))
    api(project(":catalog"))
    api("org.slf4j:slf4j-api")
    api("org.reflections:reflections")
    testImplementation("org.junit.jupiter:junit-jupiter-api")
    testImplementation("org.junit.jupiter:junit-jupiter-params")
    testImplementation("org.apache.calcite:calcite-server")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")

    // TODO: Manage all dependency versions in one place.
    antlr("org.antlr:antlr4:4.9.2")
}

tasks.generateGrammarSource {
    arguments = arguments + listOf("-visitor")
    outputDirectory = File("src/main/java/com/risingwave/parser/antlr/v4")
}