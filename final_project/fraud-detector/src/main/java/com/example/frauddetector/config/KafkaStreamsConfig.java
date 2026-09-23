package com.example.frauddetector.config;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.ReactiveKafkaProducerTemplate;
import org.springframework.kafka.core.Supplier;
import org.springframework.kafka.streams.KafkaStreamsBootstrap;
import org.springframework.kafka.streams.config.StreamsBuilderFactoryBean;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaStreamsConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.producer.properties.schema.registry.url}")
    private String schemaRegistryUrl;

    @Value("${spring.streams.application-id}")
    private String applicationId;

    @Value("${spring.streams.properties.state.dir}")
    private String stateDir;

    @Value("${spring.streams.properties.cache.max.bytes.buffering}")
    private long cacheMaxBytesBuffering;

    @Value("${spring.streams.properties.cleanup.interval.ms}")
    private long cleanupIntervalMs;

    @Bean
    public StreamsConfig streamsConfig() {
        Map<String, Object> props = new HashMap<>();

        // Basic Streams config
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // Serialization
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, "org.apache.kafka.common.serialization.Serdes$StringSerde");
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, "org.apache.kafka.common.serialization.Serdes$StringSerde");

        // Exactly-once semantics
        props.put("processing.guarantee", "exactly_once_v2");

        // RocksDB state store configuration
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        props.put("rocksdb.state.dir", stateDir);

        // RocksDB compression
        props.put("compression.type", "lz4");

        // Cache configuration
        props.put("cache.max.bytes.buffering", cacheMaxBytesBuffering);

        // Cleanup interval
        props.put("cleanup.interval.ms", cleanupIntervalMs);

        // Replication
        props.put("replication.factor", 3);

        // Schema Registry for Avro
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

        return new StreamsConfig(props);
    }
}
