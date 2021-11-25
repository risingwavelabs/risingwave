import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

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

jacoco {
    applyTo(tasks.run.get())
}

tasks.register<JacocoReport>("applicationCodeCoverageReport") {
    executionData(tasks.run.get())
    sourceSets(sourceSets.main.get())
    reports {
        xml.required.set(true)
        html.required.set(true)
    }
}

plugins {
    kotlin("jvm") version "1.5.21"
}

val compileKotlin: KotlinCompile by tasks
compileKotlin.kotlinOptions {
    jvmTarget = "1.8"
}

val compileTestKotlin: KotlinCompile by tasks
compileTestKotlin.kotlinOptions {
    jvmTarget = "1.8"
}
