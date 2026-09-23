package com.example.frauddetector.processor;

import com.example.frauddetector.config.FraudDetectorProperties;
import com.example.frauddetector.avro.FraudAlert;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Instant;

/**
 * Detects calls with abnormally long duration (> 300 seconds).
 */
@Component
public class AnomalousDurationProcessor {

    private static final Logger log = LoggerFactory.getLogger(AnomalousDurationProcessor.class);

    private final FraudDetectorProperties properties;
    private final PhoneNormalizer phoneNormalizer;

    public AnomalousDurationProcessor(FraudDetectorProperties properties, PhoneNormalizer phoneNormalizer) {
        this.properties = properties;
        this.phoneNormalizer = phoneNormalizer;
    }

    /**
     * Detect calls with duration > max-duration-seconds.
     */
    public KStream<String, FraudAlert> detect(KStream<String, String> callStream) {
        return callStream
                .mapValues(value -> {
                    Integer duration = extractDuration(value);
                    String phone = extractPhone(value);
                    String callId = extractCallId(value);

                    if (duration == null || phone == null || callId == null) {
                        return null;
                    }

                    if (duration > properties.getMaxDurationSeconds()) {
                        String normalizedPhone = phoneNormalizer.normalize(phone);
                        FraudAlert alert = new FraudAlert(
                                "ANOMALOUS_DURATION_" + callId,
                                normalizedPhone,
                                FraudAlert.FraudPattern.ANOMALOUS_DURATION,
                                1,
                                Instant.now().toString(),
                                FraudAlert.AlertSeverity.LOW
                        );
                        return KeyValue.pair(normalizedPhone, alert);
                    }
                    return null;
                })
                .filter((key, value) -> value != null);
    }

    private Integer extractDuration(String callEventJson) {
        try {
            int durIndex = callEventJson.indexOf("\"duration\"");
            if (durIndex == -1) return null;
            int colonIndex = callEventJson.indexOf(":", durIndex);
            String afterColon = callEventJson.substring(colonIndex + 1).trim();
            if (afterColon.startsWith("null") || afterColon.startsWith("\"")) {
                return null;
            }
            // Handle JSON number format
            int start = afterColon.indexOf(':') >= 0 ? afterColon.indexOf(':') + 1 : 0;
            String numStr = afterColon.substring(start).trim();
            int end = 0;
            for (int i = 0; i < numStr.length(); i++) {
                char c = numStr.charAt(i);
                if (Character.isDigit(c) || c == '-') {
                    end = i + 1;
                } else {
                    break;
                }
            }
            if (end == 0) return null;
            return Integer.parseInt(numStr.substring(0, end));
        } catch (Exception e) {
            log.warn("Failed to extract duration from call event: {}", callEventJson, e);
            return null;
        }
    }

    private String extractPhone(String callEventJson) {
        try {
            int phoneIndex = callEventJson.indexOf("\"phone\"");
            if (phoneIndex == -1) return null;
            int colonIndex = callEventJson.indexOf(":", phoneIndex);
            int quoteStart = callEventJson.indexOf("\"", colonIndex + 1);
            int quoteEnd = callEventJson.indexOf("\"", quoteStart + 1);
            if (quoteStart == -1 || quoteEnd == -1) return null;
            return callEventJson.substring(quoteStart + 1, quoteEnd);
        } catch (Exception e) {
            log.warn("Failed to extract phone from call event: {}", callEventJson, e);
            return null;
        }
    }

    private String extractCallId(String callEventJson) {
        try {
            int idIndex = callEventJson.indexOf("\"callId\"");
            if (idIndex == -1) return null;
            int colonIndex = callEventJson.indexOf(":", idIndex);
            int quoteStart = callEventJson.indexOf("\"", colonIndex + 1);
            int quoteEnd = callEventJson.indexOf("\"", quoteStart + 1);
            if (quoteStart == -1 || quoteEnd == -1) return null;
            return callEventJson.substring(quoteStart + 1, quoteEnd);
        } catch (Exception e) {
            log.warn("Failed to extract callId from call event: {}", callEventJson, e);
            return null;
        }
    }
}
