package com.example.frauddetector.stream;

import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

/**
 * Enables Kafka Streams for the fraud-detector application.
 */
@Configuration
@EnableKafkaStreams
public class FraudDetectorStreamsConfig {
}
