package com.example.frauddetector.config;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Custom Avro Serde implementation using KafkaAvroSerializer/Deserializer.
 * Wraps Confluent serializers for use with Kafka Streams.
 *
 * GenericAvroSerde is not available in kafka-avro-serializer 7.6.1,
 * so we create a custom Serde wrapper.
 */
public class AvroSerde<T> implements Serde<T> {

    private static final Logger log = LoggerFactory.getLogger(AvroSerde.class);

    private final KafkaAvroSerializer serializer;
    private final KafkaAvroDeserializer deserializer;
    private final SchemaRegistryClient schemaRegistry;

    public AvroSerde(SchemaRegistryClient schemaRegistry) {
        this.schemaRegistry = schemaRegistry;
        this.serializer = new KafkaAvroSerializer(schemaRegistry);
        this.deserializer = new KafkaAvroDeserializer(schemaRegistry, null, false);

        Map<String, Object> serializerProps = new HashMap<>();
        serializerProps.put("schema.registry.url", schemaRegistry);
        serializer.configure(serializerProps, true);

        Map<String, Object> deserializerProps = new HashMap<>();
        deserializerProps.put("schema.registry.url", schemaRegistry);
        deserializerProps.put("specific.avro.reader", true);
        deserializer.configure(deserializerProps, false);

        log.info("AvroSerde initialized with Schema Registry: {}", schemaRegistry);
    }

    @Override
    public Serializer<T> serializer() {
        return (serializer, config, topic) -> this.serializer;
    }

    @Override
    public Deserializer<T> deserializer() {
        return (deserializer, config, topic) -> this.deserializer;
    }

    public void close() {
        serializer.close();
        deserializer.close();
    }
}
