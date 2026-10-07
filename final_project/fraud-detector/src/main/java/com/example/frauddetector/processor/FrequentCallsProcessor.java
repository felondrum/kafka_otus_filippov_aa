package com.example.frauddetector.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.example.frauddetector.config.FraudDetectorProperties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;

/**
 * Processes frequent calls detection using DSL Hopping Window.
 * Detects when more than 5 calls are received from the same phone number within a 1-minute hopping window (10-second advance).
 * Uses RocksDB state store via Kafka Streams Materialized API.
 * Uses JSON (String) serialization.
 */
@Component
public class FrequentCallsProcessor {

    private static final Logger log = LoggerFactory.getLogger(FrequentCallsProcessor.class);
    private static final String STATE_STORE_NAME = "frequent-calls-count";

    private final FraudDetectorProperties properties;
    private final PhoneNormalizer phoneNormalizer;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    public FrequentCallsProcessor(
            FraudDetectorProperties properties,
            PhoneNormalizer phoneNormalizer) {
        this.properties = properties;
        this.phoneNormalizer = phoneNormalizer;
    }

    public void detect(KStream<String, String> callStream, String fraudAlertsTopic) {
        // DSL Hopping Window: group by phone, apply window, count, filter by threshold
        callStream
            // Extract and normalize phone number
            .map((key, jsonEvent) -> {
                try {
                    JsonNode node = objectMapper.readTree(jsonEvent);
                    String phone = node.has("phone") ? node.get("phone").asText() : null;
                    if (phone == null || phone.isEmpty()) {
                        return KeyValue.pair(key, null);
                    }
                    String normalizedPhone = phoneNormalizer.normalize(phone);
                    return KeyValue.pair(normalizedPhone, jsonEvent);
                } catch (Exception e) {
                    log.warn("Failed to parse event: {}", e.getMessage());
                    return KeyValue.pair(key, null);
                }
            })
            // Filter out null phones
            .filter((phone, jsonEvent) -> jsonEvent != null)
            // Group by normalized phone number
            .groupBy((phone, jsonEvent) -> phone)
            // Apply hopping window (1 min size, 10 sec advance)
            .windowedBy(TimeWindows.ofSizeAndGrace(
                Duration.ofMillis(properties.getFrequentCallsWindowSizeMs()),
                Duration.ofMillis(properties.getFrequentCallsAdvanceMs())))
            // Count calls per window (RocksDB state store)
            .count(Materialized.as(STATE_STORE_NAME))
            // Convert to KStream<Windowed<String>, Long>
            .toStream()
            // Filter by threshold
            .filter((windowedKey, count) -> {
                boolean exceeds = count > properties.getFrequentCallsThreshold();
                if (exceeds) {
                    log.info("Frequent calls detected for phone: {}, count: {}, window: {}",
                        windowedKey.key(), count, windowedKey.window());
                }
                return exceeds;
            })
            // Map to FraudAlert JSON
            .map((windowedKey, count) -> {
                try {
                    ObjectNode alert = objectMapper.createObjectNode();
                    alert.put("callId", "freq-" + windowedKey.key() + "-" + windowedKey.window().start());
                    alert.put("phone", windowedKey.key());
                    alert.put("pattern", "FREQUENT_CALLS");
                    alert.put("count", count.intValue());
                    alert.put("timestamp", Instant.now().toString());
                    alert.put("severity", "MEDIUM");
                    return KeyValue.pair(windowedKey.key(), objectMapper.writeValueAsString(alert));
                } catch (Exception e) {
                    log.error("Failed to create fraud alert: {}", e.getMessage());
                    return KeyValue.pair(windowedKey.key(), null);
                }
            })
            // Write to fraud-alerts topic
            .to(fraudAlertsTopic, Produced.with(Serdes.String(), Serdes.String()));
    }
}
