package com.example.frauddetector.config;

import com.example.frauddetector.processor.AnomalousDurationProcessor;
import com.example.frauddetector.processor.FrequentCallsProcessor;
import com.example.frauddetector.processor.NpsEscalationProcessor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Consumed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Kafka Streams topology configuration for fraud detection.
 * Uses JSON (String) serialization.
 */
@Configuration
public class StreamsTopologyConfig {

    private static final Logger log = LoggerFactory.getLogger(StreamsTopologyConfig.class);

    private final FraudDetectorProperties properties;
    private final FrequentCallsProcessor frequentCallsProcessor;
    private final NpsEscalationProcessor npsEscalationProcessor;
    private final AnomalousDurationProcessor anomalousDurationProcessor;

    public StreamsTopologyConfig(
            FraudDetectorProperties properties,
            FrequentCallsProcessor frequentCallsProcessor,
            NpsEscalationProcessor npsEscalationProcessor,
            AnomalousDurationProcessor anomalousDurationProcessor) {
        this.properties = properties;
        this.frequentCallsProcessor = frequentCallsProcessor;
        this.npsEscalationProcessor = npsEscalationProcessor;
        this.anomalousDurationProcessor = anomalousDurationProcessor;
    }

    @Bean("defaultKafkaStreamsConfig")
    public org.springframework.kafka.config.KafkaStreamsConfiguration kafkaStreamsConfiguration(
            @Value("${KAFKA_BOOTSTRAP_SERVERS:kafka-1:9092,kafka-2:9092,kafka-3:9092}") String bootstrapServers,
            @Value("${APP_ID:fraud-detector}") String appId,
            @Value("${STATE_DIR:/tmp/kafka-streams}") String stateDir) {

        Map<String, Object> props = new HashMap<>();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("application.id", appId);
        props.put("state.dir", stateDir);
        props.put("default.key.serde", "org.apache.kafka.common.serialization.Serdes$StringSerde");
        props.put("default.value.serde", "org.apache.kafka.common.serialization.Serdes$StringSerde");
        props.put("processing.guarantee", "at_least_once");
        props.put("cache.max.bytes.buffering", 10485760);
        props.put("cleanup.interval.ms", 60000);

        return new org.springframework.kafka.config.KafkaStreamsConfiguration(props);
    }

    @Bean
    public StreamsBuilder streamsBuilder() {
        return new StreamsBuilder();
    }

    @Bean
    public KafkaStreams kafkaStreams(StreamsBuilder streamsBuilder,
                                     FraudDetectorProperties properties,
                                     @Value("${APP_ID:fraud-detector}") String appId,
                                     @Value("${STATE_DIR:/tmp/kafka-streams}") String stateDir) {
        // Build properties
        Map<String, Object> props = new HashMap<>();
        String bootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS") != null ? 
                System.getenv("KAFKA_BOOTSTRAP_SERVERS") : "kafka-1:9092,kafka-2:9092,kafka-3:9092";
        props.put("bootstrap.servers", bootstrapServers);
        props.put("application.id", appId);
        props.put("state.dir", stateDir);
        props.put("default.key.serde", "org.apache.kafka.common.serialization.Serdes$StringSerde");
        props.put("default.value.serde", "org.apache.kafka.common.serialization.Serdes$StringSerde");
        props.put("processing.guarantee", "at_least_once");
        props.put("cache.max.bytes.buffering", 10485760);
        props.put("cleanup.interval.ms", 60000);

        Properties kafkaStreamsProps = new Properties();
        props.forEach(kafkaStreamsProps::put);

        // Input: calls.completed (String/JSON)
        String completedTopic = properties.getCompletedTopic();
        log.info("Building Kafka Streams topology with topic: {}", completedTopic);
        
        if (completedTopic == null || completedTopic.isEmpty()) {
            throw new IllegalStateException("completedTopic cannot be null or empty");
        }
        
        KStream<String, String> callStream = streamsBuilder
                .stream(completedTopic,
                        Consumed.with(Serdes.String(), Serdes.String()));

        // Short call filter: filter out calls with duration < 5 seconds
        KStream<String, String> validCallStream = callStream.filter((key, jsonEvent) -> {
            try {
                com.fasterxml.jackson.databind.JsonNode node = new com.fasterxml.jackson.databind.ObjectMapper().readTree(jsonEvent);
                com.fasterxml.jackson.databind.JsonNode durationNode = node.get("duration");
                int duration = durationNode != null && !durationNode.isNull() ? durationNode.asInt() : 0;
                boolean passes = duration >= 5;
                if (!passes) {
                    log.debug("Filtered out short call: key={}, duration={}", key, duration);
                }
                return passes;
            } catch (Exception e) {
                log.warn("Failed to parse event for short call filter: {}", e.getMessage());
                return true; // process on next pass
            }
        });

        // Add state store for NPS escalation
        streamsBuilder.addStateStore(
            org.apache.kafka.streams.state.Stores.keyValueStoreBuilder(
                org.apache.kafka.streams.state.Stores.persistentKeyValueStore("nps-escalation-store"),
                Serdes.String(),
                Serdes.String()
            )
        );

        // Process through fraud detection pipelines
        frequentCallsProcessor.detect(validCallStream, properties.getFraudAlertsTopic());
        npsEscalationProcessor.detect(validCallStream, properties.getFraudAlertsTopic());
        anomalousDurationProcessor.detect(validCallStream, properties.getFraudAlertsTopic());

        log.info("Kafka Streams topology built successfully with application.id: {}", appId);
        return new KafkaStreams(streamsBuilder.build(), kafkaStreamsProps);
    }
}
