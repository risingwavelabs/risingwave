import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.5.21"
}

dependencies {
    // Get recommended versions from platform project
    api(platform(project(":bom")))

    api("com.google.guava:guava")
    api(project(":common"))
    api("org.apache.calcite:calcite-core")
    api("org.slf4j:slf4j-api")
    api("org.reflections:reflections")
    implementation("com.google.inject:guice")
    implementation(project(":proto"))
    implementation(kotlin("stdlib-jdk8"))
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core")
    api("com.google.protobuf:protobuf-java-util")
}

val compileKotlin: KotlinCompile by tasks
compileKotlin.kotlinOptions {
    jvmTarget = "1.8"
}
val compileTestKotlin: KotlinCompile by tasks
compileTestKotlin.kotlinOptions {
    jvmTarget = "1.8"
}
