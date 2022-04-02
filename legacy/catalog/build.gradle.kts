dependencies {
    api(platform(project(":bom")))

    api("com.google.guava:guava")
    api(project(":common"))
    api(project(":meta"))
    api(project(":proto"))
    api("org.apache.calcite:calcite-core")
    api("org.apache.commons:commons-lang3")
    implementation("com.google.inject:guice")
}
