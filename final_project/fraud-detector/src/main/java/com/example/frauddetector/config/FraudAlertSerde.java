package com.example.frauddetector.config;

import com.example.frauddetector.avro.FraudAlert;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Simple Avro Serde for FraudAlert that uses built-in binary encoder/decoder.
 * Works without Schema Registry connection - perfect for tests.
 */
@SuppressWarnings("unchecked")
public class FraudAlertSerde implements Serde<FraudAlert> {

    private static final Logger log = LoggerFactory.getLogger(FraudAlertSerde.class);
    private final FraudAlertSerializer serializer = new FraudAlertSerializer();
    private final FraudAlertDeserializer deserializer = new FraudAlertDeserializer();

    public FraudAlertSerde() {
        log.info("FraudAlertSerde initialized (no Schema Registry required)");
    }

    @Override
    public Serializer<FraudAlert> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<FraudAlert> deserializer() {
        return deserializer;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // No configuration needed
    }

    @Override
    public void close() {
        // No resources to close
    }

    /**
     * Uses FraudAlert.toByteBuffer() for serialization.
     */
    private static class FraudAlertSerializer implements Serializer<FraudAlert> {
        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            // No configuration needed
        }

        @Override
        public byte[] serialize(String topic, FraudAlert data) {
            if (data == null) {
                return null;
            }
            try {
                return data.toByteBuffer().array();
            } catch (IOException e) {
                throw new RuntimeException("Failed to serialize FraudAlert", e);
            }
        }

        @Override
        public void close() {
            // No resources to close
        }
    }

    /**
     * Uses FraudAlert.fromByteBuffer() for deserialization.
     */
    private static class FraudAlertDeserializer implements Deserializer<FraudAlert> {
        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            // No configuration needed
        }

        @Override
        public FraudAlert deserialize(String topic, byte[] data) {
            if (data == null) {
                return null;
            }
            try {
                return FraudAlert.fromByteBuffer(java.nio.ByteBuffer.wrap(data));
            } catch (IOException e) {
                throw new RuntimeException("Failed to deserialize FraudAlert", e);
            }
        }

        @Override
        public void close() {
            // No resources to close
        }
    }
}
