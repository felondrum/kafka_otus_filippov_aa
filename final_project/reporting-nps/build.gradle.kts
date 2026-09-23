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
    maven { url = uri("https://packages.confluent.io/maven/") }
}

val avroVersion = "1.12.0"

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
    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.boot:spring-boot-starter-validation")
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("org.springframework.boot:spring-boot-starter-data-jpa")

    // Kafka
    implementation("org.springframework.kafka:spring-kafka")

    // Schema Registry & Avro
    implementation("io.confluent:kafka-schema-registry-client:7.6.1")
    implementation("io.confluent:kafka-avro-serializer:7.6.1")
    implementation("org.apache.avro:avro:1.12.0")

    // PostgreSQL
    implementation("org.postgresql:postgresql:42.7.3")

    // Caffeine Cache
    implementation("com.github.ben-manes.caffeine:caffeine:3.1.8")

    // Spring Retry
    implementation("org.springframework.retry:spring-retry")
    implementation("org.springframework:spring-aspects")

    // Micrometer & Prometheus
    implementation("io.micrometer:micrometer-registry-prometheus")

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
    finalizedBy(tasks.jacocoTestReport)
}

tasks.register("jacocoTestReport") {
    dependsOn(tasks.test, tasks.jacocoTestReportExecution)
    doLast {
        val report = tasks.named("jacocoTestReport").get()
        println("JaCoCo report generated at: ${report.extensions.extraProperties.get("reports")?.let { (it as? Map<*>)?.get("xml")?.let { xml -> (xml as? Map<*>)?.get("location") } } ?: "check tasks.jacocoTestReport output"}")
    }
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

jacocoTestReport {
    dependsOn(tasks.test)
    reports {
        xml.required.set(true)
        html.required.set(true)
    }
}
