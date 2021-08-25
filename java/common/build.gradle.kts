dependencies {
    // Get recommended versions from platform project
    api(platform(project(":bom")))

    implementation(project(":proto"))

    // Add dependencies
    api("org.apache.calcite:calcite-core")

    testImplementation("org.junit.jupiter:junit-jupiter-api")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
}
