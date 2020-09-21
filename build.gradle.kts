plugins {
    kotlin("jvm") version "1.4.10"
}


group = "com.findinpath"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    // logging
    implementation("org.slf4j:slf4j-api:1.7.30")
    implementation("org.apache.logging.log4j:log4j-slf4j-impl:2.12.0")
    implementation("org.apache.logging.log4j:log4j-api:2.11.2")
    implementation("org.apache.logging.log4j:log4j-core:2.11.2")




    implementation("org.elasticsearch:elasticsearch:7.9.1")
    implementation("org.elasticsearch.client:elasticsearch-rest-high-level-client:7.9.1")


    testImplementation("org.junit.jupiter:junit-jupiter-api:5.7.0")
    testImplementation("org.junit.jupiter:junit-jupiter-engine:5.7.0")

    testImplementation("org.testcontainers:junit-jupiter:1.14.3")
    testImplementation("org.testcontainers:elasticsearch:1.14.3")


}

tasks.withType<Test>().configureEach {
    useJUnitPlatform()
}