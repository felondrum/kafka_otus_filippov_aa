package com.example.frauddetector.config;

import org.apache.kafka.streams.KafkaStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import jakarta.annotation.PreDestroy;

/**
 * Manages the lifecycle of the KafkaStreams instance.
 * Starts the streams when the application context is ready and stops gracefully on shutdown.
 */
@Component
public class KafkaStreamsLifecycle {

    private static final Logger log = LoggerFactory.getLogger(KafkaStreamsLifecycle.class);

    private final KafkaStreams kafkaStreams;

    public KafkaStreamsLifecycle(KafkaStreams kafkaStreams) {
        this.kafkaStreams = kafkaStreams;
    }

    /**
     * Start the Kafka Streams instance when the application is ready.
     */
    @jakarta.annotation.PostConstruct
    public void start() {
        log.info("Starting Kafka Streams instance...");
        kafkaStreams.start();
        log.info("Kafka Streams instance started successfully");
    }

    /**
     * Gracefully stop the Kafka Streams instance on shutdown.
     */
    @PreDestroy
    public void stop() {
        log.info("Stopping Kafka Streams instance...");
        kafkaStreams.close();
        log.info("Kafka Streams instance stopped successfully");
    }
}
