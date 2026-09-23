package com.example.frauddetector.processor;

import com.example.frauddetector.config.FraudDetectorProperties;
import com.example.frauddetector.avro.FraudAlert;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.apache.kafka.streams.kstream.Windows;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Serialized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Instant;

/**
 * Processes frequent calls detection using Kafka Streams DSL with hopping windows.
 * Detects when more than N calls are received from the same phone number
 * within a 1-minute hopping window (10-second advance).
 */
@Component
public class FrequentCallsProcessor {

    private static final Logger log = LoggerFactory.getLogger(FrequentCallsProcessor.class);

    private final FraudDetectorProperties properties;
    private final PhoneNormalizer phoneNormalizer;

    public FrequentCallsProcessor(FraudDetectorProperties properties, PhoneNormalizer phoneNormalizer) {
        this.properties = properties;
        this.phoneNormalizer = phoneNormalizer;
    }

    /**
     * Apply frequent calls detection to a KStream of CallEvent.
     * Returns a KStream of FraudAlert.
     */
    public KStream<String, FraudAlert> detect(KStream<String, String> callStream) {
        Windows<String> windows = TimeWindows.ofSizeAndAdvance(
                properties.getFrequentCallsWindowSizeMs(),
                properties.getFrequentCallsAdvanceMs()
        ).grace(org.apache.kafka.streams.kstream.TimeWindows.MAX_GRACE);

        return callStream
                .map((key, value) -> {
                    // Parse the call event and normalize phone
                    String phone = extractPhone(value);
                    if (phone == null) {
                        return null;
                    }
                    String normalizedPhone = phoneNormalizer.normalize(phone);
                    return org.apache.kafka.common.record.NewRecordRecord.with(
                            normalizedPhone, value
                    );
                })
                .filter((key, value) -> value != null)
                .selectKey((key, value) -> key)
                .groupBy((phone, value) -> phone)
                .windowedBy(windows)
                .count()
                .filter((windowedKey, count) -> count > properties.getFrequentCallsThreshold())
                .toStream()
                .map((windowedKey, count) -> {
                    String callId = "FREQUENT_CALLS_" + windowedKey.key();
                    String timestamp = Instant.now().toString();
                    FraudAlert alert = new FraudAlert(
                            callId,
                            windowedKey.key(),
                            FraudAlert.FraudPattern.FREQUENT_CALLS,
                            count.intValue(),
                            timestamp,
                            FraudAlert.AlertSeverity.MEDIUM
                    );
                    return org.apache.kafka.common.record.NewRecordRecord.with(
                            windowedKey.key(), alert
                    );
                })
                .filter((key, value) -> value != null);
    }

    private String extractPhone(String callEventJson) {
        try {
            // Parse JSON to extract phone field
            // Simple JSON parsing - extract "phone" field value
            int phoneIndex = callEventJson.indexOf("\"phone\"");
            if (phoneIndex == -1) {
                return null;
            }
            int colonIndex = callEventJson.indexOf(":", phoneIndex);
            int quoteStart = callEventJson.indexOf("\"", colonIndex + 1);
            int quoteEnd = callEventJson.indexOf("\"", quoteStart + 1);
            if (quoteStart == -1 || quoteEnd == -1) {
                return null;
            }
            return callEventJson.substring(quoteStart + 1, quoteEnd);
        } catch (Exception e) {
            log.warn("Failed to extract phone from call event: {}", callEventJson, e);
            return null;
        }
    }
}
