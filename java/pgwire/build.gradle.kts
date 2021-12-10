import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.5.21"
}

dependencies {
    // Get recommended versions from platform project
    api(platform(project(":bom")))

    api("org.slf4j:slf4j-api")

    implementation(project(":common"))
    implementation(project(":proto"))
    implementation(kotlin("stdlib-jdk8"))
    implementation("io.ktor:ktor-network")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core")
    implementation("io.netty:netty-buffer")
    implementation("com.google.inject:guice")

    testImplementation("org.duckdb:duckdb_jdbc")
    testRuntimeOnly("ch.qos.logback:logback-classic")
    testImplementation("org.junit.jupiter:junit-jupiter-api")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
}

val compileKotlin: KotlinCompile by tasks
compileKotlin.kotlinOptions {
    jvmTarget = "1.8"
}
val compileTestKotlin: KotlinCompile by tasks
compileTestKotlin.kotlinOptions {
    jvmTarget = "1.8"
}
