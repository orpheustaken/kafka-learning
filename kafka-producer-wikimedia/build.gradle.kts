plugins {
    id("java")
}

group = "com.github.orpheustaken"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.apache.kafka:kafka-clients:3.4.0")
    implementation("org.slf4j:slf4j-api:2.0.7")
    implementation("org.slf4j:slf4j-simple:2.0.7")
    implementation("com.squareup.okhttp3:okhttp:4.10.0")
    implementation("com.launchdarkly:okhttp-eventsource:4.1.0")
}

tasks.test {
}
