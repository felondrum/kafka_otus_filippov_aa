package com.example.frauddetector.processor;

import com.example.frauddetector.config.FraudDetectorProperties;
import com.example.frauddetector.avro.FraudAlert;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Processor for NPS escalation detection using Kafka Streams Processor API.
 * Detects when a phone number has 3+ negative NPS scores (NPS < 2) within 24 hours.
 */
@Component
public class NpsEscalationProcessor {

    private static final Logger log = LoggerFactory.getLogger(NpsEscalationProcessor.class);

    private final FraudDetectorProperties properties;
    private final PhoneNormalizer phoneNormalizer;

    public NpsEscalationProcessor(FraudDetectorProperties properties, PhoneNormalizer phoneNormalizer) {
        this.properties = properties;
        this.phoneNormalizer = phoneNormalizer;
    }

    /**
     * Create a KStream processor for NPS escalation detection.
     * Returns a KStream of FraudAlert.
     */
    public KStream<String, FraudAlert> detect(KStream<String, String> callStream) {
        return callStream
                .mapValues(value -> {
                    String phone = extractPhone(value);
                    Integer npsScore = extractNpsScore(value);
                    if (phone == null || npsScore == null) {
                        return null;
                    }
                    String normalizedPhone = phoneNormalizer.normalize(phone);
                    return KeyValue.pair(normalizedPhone, npsScore);
                })
                .filter((key, npsScore) -> key != null && npsScore != null && npsScore < 2)
                .process(() -> new NpsEscalationProcessorSupplier(properties, phoneNormalizer), "nps-escalation-process");
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

    private Integer extractNpsScore(String callEventJson) {
        try {
            int npsIndex = callEventJson.indexOf("\"npsScore\"");
            if (npsIndex == -1) return null;
            int colonIndex = callEventJson.indexOf(":", npsIndex);
            String afterColon = callEventJson.substring(colonIndex + 1).trim();
            if (afterColon.startsWith("null") || afterColon.startsWith("\"")) {
                return null;
            }
            int start = afterColon.indexOf(':') >= 0 ? afterColon.indexOf(':') + 1 : 0;
            String numStr = afterColon.substring(start).trim();
            // Handle JSON number format
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
            log.warn("Failed to extract NPS score from call event: {}", callEventJson, e);
            return null;
        }
    }

    /**
     * Processor that tracks NPS escalation per phone number.
     */
    public static class NpsEscalationProcessor extends AbstractProcessor<String, Integer> {
        private final FraudDetectorProperties properties;
        private final PhoneNormalizer phoneNormalizer;
        private ProcessorContext context;
        private StateStore npsCounterStore;

        public NpsEscalationProcessor(FraudDetectorProperties properties, PhoneNormalizer phoneNormalizer) {
            this.properties = properties;
            this.phoneNormalizer = phoneNormalizer;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void init(ProcessorContext context) {
            this.context = context;
            this.npsCounterStore = context.getStateStore("nps-counter-store");
            context.schedule(
                    properties.getNpsEscalationWindowHours() * 3600_000L,
                    PunctuationType.WALL_CLOCK_TIME,
                    timestamp -> cleanupExpiredEntries()
            );
        }

        @Override
        @SuppressWarnings("unchecked")
        public void process(String phone, Integer npsScore) {
            if (npsCounterStore == null) {
                log.warn("NPS counter store not available");
                return;
            }

            NpsCounter counter = (NpsCounter) npsCounterStore.get(phone);
            if (counter == null) {
                counter = new NpsCounter();
            }

            counter.addNegativeNps(npsScore, System.currentTimeMillis());

            if (counter.getNegativeCount() >= properties.getNpsEscalationThreshold()) {
                FraudAlert alert = new FraudAlert(
                        "NPS_ESCALATION_" + phone,
                        phone,
                        FraudAlert.FraudPattern.NPS_ESCALATION,
                        counter.getNegativeCount(),
                        Instant.now().toString(),
                        FraudAlert.AlertSeverity.HIGH
                );
                context.forward("fraud-alerts", KeyValue.pair(phone, alert));
                // Reset counter after alert
                counter.reset();
            }

            npsCounterStore.put(phone, counter);
        }

        private void cleanupExpiredEntries() {
            if (npsCounterStore == null) return;
            long cutoff = System.currentTimeMillis() - (long) properties.getNpsEscalationWindowHours() * 3600_000L;
            // Iterate and remove expired entries
            try (KeyValueIterator<byte[], byte[]> iter = npsCounterStore.all()) {
                while (iter.hasNext()) {
                    KeyValue<byte[], byte[]> entry = iter.next();
                    String phone = new String(entry.key);
                    NpsCounter counter = (NpsCounter) npsCounterStore.get(phone);
                    if (counter != null && counter.getLastActivityTime() < cutoff) {
                        npsCounterStore.delete(phone);
                    }
                }
            }
        }

        @Override
        public void close() {
            // cleanup
        }
    }

    /**
     * State store value for NPS counter.
     */
    public static class NpsCounter {
        private int negativeCount;
        private long lastActivityTime;

        public NpsCounter() {
            this.negativeCount = 0;
            this.lastActivityTime = 0;
        }

        public void addNegativeNps(int npsScore, long timestamp) {
            this.negativeCount++;
            this.lastActivityTime = Math.max(this.lastActivityTime, timestamp);
        }

        public int getNegativeCount() {
            return negativeCount;
        }

        public long getLastActivityTime() {
            return lastActivityTime;
        }

        public void reset() {
            this.negativeCount = 0;
            this.lastActivityTime = 0;
        }
    }

    /**
     * Processor supplier for NPS escalation.
     */
    public static class NpsEscalationProcessorSupplier implements ProcessorSupplier<String, Integer> {
        private final FraudDetectorProperties properties;
        private final PhoneNormalizer phoneNormalizer;

        public NpsEscalationProcessorSupplier(FraudDetectorProperties properties, PhoneNormalizer phoneNormalizer) {
            this.properties = properties;
            this.phoneNormalizer = phoneNormalizer;
        }

        @Override
        public Processor<String, Integer> get() {
            return new NpsEscalationProcessor(properties, phoneNormalizer);
        }
    }
}
