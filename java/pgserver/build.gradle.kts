dependencies {
  implementation(platform(project(":bom")))

  implementation(project(":common"))
  implementation(project(":pgwire"))
  implementation(project(":catalog"))
  implementation(project(":planner"))
  implementation("com.github.pcj:google-options")
  implementation("com.google.inject:guice")

  testImplementation("org.junit.jupiter:junit-jupiter-api")
  testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
}

application {
  mainClass.set("com.risingwave.pgserver.FrontendServer")
  tasks.named<JavaExec>("run") {
    val defaultConfigFile = "${project.projectDir}/src/main/resources/server.properties"
    setArgsString("-c $defaultConfigFile")
  }
}