dependencies {
    implementation(platform(project(":bom")))

    implementation(project(":common"))
    implementation(project(":pgwire"))
    implementation(project(":catalog"))
    implementation(project(":planner"))
    implementation("com.github.pcj:google-options")

    testImplementation("org.junit.jupiter:junit-jupiter-api")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
}