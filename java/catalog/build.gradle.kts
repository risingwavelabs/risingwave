dependencies {
    api(platform(project(":bom")))

    api("com.google.guava:guava")
    api(project(":common"))
    api(project(":metadata"))
    api(project(":proto"))
    api("org.apache.calcite:calcite-core")
    implementation("com.google.inject:guice")
}
