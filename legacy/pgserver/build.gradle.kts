import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
    application
    id("com.github.johnrengelman.shadow").version("7.1.0")
    kotlin("jvm")
}

val pgServerMainClass = "com.risingwave.pgserver.FrontendServer"

configurations {
    project.setProperty("mainClassName", pgServerMainClass)
    tasks.shadowDistZip.get().enabled = false
    tasks.shadowDistTar.get().enabled = false
    tasks.startShadowScripts.get().enabled = false
    tasks.distTar.get().enabled = false
    tasks.distZip.get().enabled = false
}

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
    mainClass.set(pgServerMainClass)
    tasks.named<JavaExec>("run") {
        val defaultConfigFile = "${project.projectDir}/src/main/resources/server.properties"
        setArgsString("-c $defaultConfigFile")
    }
}

tasks.register<JacocoReport>("applicationCodeCoverageReport") {
    executionData(tasks.run.get())
    sourceSets(sourceSets.main.get())
    reports {
        xml.required.set(true)
        html.required.set(true)
    }
}

tasks.named<ShadowJar>("shadowJar") {
    archiveFileName.set("risingwave-fe-runnable.jar")
    destinationDirectory.set(file("build/libs/"))
    mergeServiceFiles()
    excludes.addAll(listOf("logback.xml", "server.properties"))
}

val compileKotlin: KotlinCompile by tasks
compileKotlin.kotlinOptions {
    jvmTarget = "11"
}

val compileTestKotlin: KotlinCompile by tasks
compileTestKotlin.kotlinOptions {
    jvmTarget = "11"
}