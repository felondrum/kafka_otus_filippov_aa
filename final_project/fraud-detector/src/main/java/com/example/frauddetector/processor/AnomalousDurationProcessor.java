package com.example.frauddetector.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.example.frauddetector.config.FraudDetectorProperties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Instant;

/**
 * Detects calls with abnormally long duration (> 300 seconds).
 * Produces FraudAlert with severity=LOW.
 * Uses JSON (String) serialization.
 */
@Component
public class AnomalousDurationProcessor {

    private static final Logger log = LoggerFactory.getLogger(AnomalousDurationProcessor.class);

    private final FraudDetectorProperties properties;
    private final PhoneNormalizer phoneNormalizer;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    public AnomalousDurationProcessor(
            FraudDetectorProperties properties,
            PhoneNormalizer phoneNormalizer) {
        this.properties = properties;
        this.phoneNormalizer = phoneNormalizer;
    }

    public void detect(KStream<String, String> callStream, String fraudAlertsTopic) {
        // Filter calls with duration > max-duration-seconds
        callStream.filter((key, jsonEvent) -> {
            try {
                JsonNode node = objectMapper.readTree(jsonEvent);
                JsonNode durationNode = node.get("duration");
                Integer duration = durationNode != null && !durationNode.isNull() ? durationNode.asInt() : null;
                return duration != null && duration > properties.getMaxDurationSeconds();
            } catch (Exception e) {
                log.warn("Failed to parse event: {}", e.getMessage());
                return false;
            }
        })
        .map((key, jsonEvent) -> {
            try {
                JsonNode node = objectMapper.readTree(jsonEvent);
                String phone = node.has("phone") ? node.get("phone").asText() : null;
                String callId = node.has("callId") ? node.get("callId").asText() : null;
                JsonNode durationNode = node.get("duration");
                Integer duration = durationNode != null && !durationNode.isNull() ? durationNode.asInt() : null;

                if (phone == null || phone.isEmpty() || callId == null || callId.isEmpty()) {
                    return KeyValue.pair(key, null);
                }

                String normalizedPhone = phoneNormalizer.normalize(phone);
                ObjectNode alert = objectMapper.createObjectNode();
                alert.put("callId", "anom-" + callId + "-" + System.currentTimeMillis());
                alert.put("phone", normalizedPhone);
                alert.put("pattern", "ANOMALOUS_DURATION");
                alert.put("count", 1);
                alert.put("timestamp", Instant.now().toString());
                alert.put("severity", "LOW");
                return KeyValue.pair(normalizedPhone, objectMapper.writeValueAsString(alert));
            } catch (Exception e) {
                log.error("Error creating alert: {}", e.getMessage());
                return KeyValue.pair(key, null);
            }
        })
        .filter((phone, alert) -> alert != null)
        .to(fraudAlertsTopic, Produced.with(Serdes.String(), Serdes.String()));
    }
}
