dependencies {
    // Get recommended versions from platform project
    api(platform(project(":bom")))

    // Add dependencies
    api("org.apache.calcite:calcite-core")
}
