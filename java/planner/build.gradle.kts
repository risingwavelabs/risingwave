plugins {
    antlr
}

dependencies {
    // Get recommended versions from platform project
    api(platform(project(":bom")))

    // Declare dependencies, no version required
    api("org.apache.calcite:calcite-core")

    // TODO: Manage all dependency versions in one place.
    antlr("org.antlr:antlr4:4.9.2")
}

tasks.generateGrammarSource {
    arguments = arguments + listOf("-visitor")
    outputDirectory = File("src/main/java/com/risingwave/parser/antlr/v4")
}