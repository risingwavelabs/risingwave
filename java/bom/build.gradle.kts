plugins {
    `java-platform`
}

dependencies {
    constraints {
        api("org.apache.calcite:calcite-core:1.27.0")
        api("com.google.guava:guava:30.1.1-jre")
    }
}
