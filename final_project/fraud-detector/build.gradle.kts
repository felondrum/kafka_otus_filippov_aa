plugins {
    java
    id("org.springframework.boot") version "3.2.3"
    id("io.spring.dependency-management") version "1.1.4"
    id("com.github.johnshane.avro-generator") version "0.1.0"
    id("com.github.davidmc24.gradle.plugin.avro") version "1.9.1"
}

group = "com.example"
version = "1.0.0-SNAPSHOT"

java {
    sourceCompatibility = JavaVersion.VERSION_21
    targetCompatibility = JavaVersion.VERSION_21
}

repositories {
    mavenCentral()
    maven { url = uri("https://packages.confluent.io/maven/") }
    maven { url = uri("https://repo.spring.io/milestone") }
}

val avroVersion = "1.12.0"
val confluentVersion = "7.6.1"

configurations {
    compileClassPath {
        resolutionStrategy.eachDependency {
            if (requested.group == "org.apache.avro" && requested.name.startsWith("avro")) {
                useVersion(avroVersion)
                because("Force consistent Avro version")
            }
        }
    }
}

dependencies {
    // Spring Boot
    implementation("org.springframework.boot:spring-boot-starter")
    implementation("org.springframework.boot:spring-boot-starter-actuator")

    // Kafka
    implementation("org.springframework.kafka:spring-kafka")
    implementation("org.apache.kafka:kafka-streams")

    // Schema Registry & Avro
    implementation("io.confluent:kafka-schema-registry-client:$confluentVersion")
    implementation("io.confluent:kafka-avro-serializer:$confluentVersion")
    implementation("org.apache.avro:avro:$avroVersion")

    // Testing
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.springframework.kafka:spring-kafka-test")
    testImplementation("org.mockito:mockito-core:5.14.2")
    testImplementation("org.mockito:mockito-junit-jupiter:5.14.2")
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.2")
    testImplementation("org.testcontainers:kafka:1.19.3")
    testImplementation("org.testcontainers:junit-jupiter:1.19.3")
    testImplementation("org.testcontainers:postgresql:1.19.3")
    testImplementation("org.awaitility:awaitility:4.2.0")

    // Logging
    implementation("net.logstash.logback:logstash-logback-encoder:7.4")
}

tasks.test {
    useJUnitPlatform()
    systemProperty("spring.profiles.active", "test")
}

tasks.bootBuildImage {
    imageName.set("fraud-detector:1.0.0-SNAPSHOT")
}

tasks.withType<JavaCompile> {
    options.encoding = "UTF-8"
}

avro {
    fieldVisibility = "PRIVATE"
    stringFields = listOf("callId", "phone", "agentId", "status")
    enableDecimalPadding = false
}
