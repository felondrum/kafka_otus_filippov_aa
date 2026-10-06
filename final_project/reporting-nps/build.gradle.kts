plugins {
    java
    id("org.springframework.boot") version "3.2.3"
    id("io.spring.dependency-management") version "1.1.4"
    id("jacoco")
}

group = "com.example"
version = "1.0.0-SNAPSHOT"

java {
    sourceCompatibility = JavaVersion.VERSION_21
    targetCompatibility = JavaVersion.VERSION_21
}

repositories {
    mavenCentral()
}

dependencies {
    // Spring Boot
    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.boot:spring-boot-starter-validation")
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("org.springframework.boot:spring-boot-starter-data-jpa")

    // Kafka
    implementation("org.springframework.kafka:spring-kafka")

    // PostgreSQL
    implementation("org.postgresql:postgresql:42.7.3")

    // Caffeine Cache
    implementation("com.github.ben-manes.caffeine:caffeine:3.1.8")

    // Spring Retry
    implementation("org.springframework.retry:spring-retry")
    implementation("org.springframework:spring-aspects")

    // Micrometer & Prometheus
    implementation("io.micrometer:micrometer-registry-prometheus")

    // Swagger UI / OpenAPI
    implementation("org.springdoc:springdoc-openapi-starter-webmvc-ui:2.3.0")

    // Jackson
    implementation("com.fasterxml.jackson.core:jackson-databind")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310")

    // Logging
    implementation("net.logstash.logback:logstash-logback-encoder:7.4")

    // Test dependencies
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.mockito:mockito-core:5.14.2")
    testImplementation("org.springframework.kafka:spring-kafka-test")
    testImplementation("org.testcontainers:kafka:1.19.3")
    testImplementation("org.testcontainers:junit-jupiter:1.19.3")
    testImplementation("org.testcontainers:postgresql:1.19.3")
    testImplementation("org.awaitility:awaitility:4.2.0")
}

tasks.test {
    useJUnitPlatform()
    systemProperty("spring.profiles.active", "test")
}

tasks.withType<JavaCompile> {
    options.encoding = "UTF-8"
}

tasks.bootBuildImage {
    imageName.set("reporting-nps:1.0.0-SNAPSHOT")
}

jacoco {
    toolVersion = "0.8.11"
}
