package com.example.frauddetector.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.example.frauddetector.config.FraudDetectorProperties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

/**
 * Processor for NPS escalation detection using Processor API with StateStore.
 * Detects when a phone number has 3 or more negative NPS scores (NPS < 2) within a 24-hour period.
 * Uses RocksDB KeyValueStore for per-phone state persistence.
 * Uses JSON (String) serialization.
 */
@Component
public class NpsEscalationProcessor {

    private static final Logger log = LoggerFactory.getLogger(NpsEscalationProcessor.class);
    public static final String STATE_STORE_NAME = "nps-escalation-store";

    private final FraudDetectorProperties properties;
    private final PhoneNormalizer phoneNormalizer;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public String getStateStoreName() {
        return STATE_STORE_NAME;
    }

    @Autowired
    public NpsEscalationProcessor(
            FraudDetectorProperties properties,
            PhoneNormalizer phoneNormalizer) {
        this.properties = properties;
        this.phoneNormalizer = phoneNormalizer;
    }

    public void detect(KStream<String, String> callStream, String fraudAlertsTopic) {
        // Use Processor API with StateStore
        callStream
            // Filter events with valid phone and NPS < 2
            .filter((key, jsonEvent) -> {
                try {
                    JsonNode node = objectMapper.readTree(jsonEvent);
                    JsonNode phoneNode = node.get("phone");
                    JsonNode npsNode = node.get("npsScore");
                    String phone = phoneNode != null ? phoneNode.asText() : null;
                    Integer npsScore = npsNode != null && !npsNode.isNull() ? npsNode.asInt() : null;
                    return phone != null && !phone.isEmpty() && npsScore != null && npsScore < 2;
                } catch (Exception e) {
                    log.warn("Failed to parse event: {}", e.getMessage());
                    return false;
                }
            })
            // Map to normalized phone
            .map((key, jsonEvent) -> {
                try {
                    JsonNode node = objectMapper.readTree(jsonEvent);
                    String phone = node.get("phone").asText();
                    String normalizedPhone = phoneNormalizer.normalize(phone);
                    return KeyValue.pair(normalizedPhone, jsonEvent);
                } catch (Exception e) {
                    return KeyValue.pair(key, null);
                }
            })
            // Process with custom Processor + StateStore
            .transform(new TransformerSupplier<String, String, KeyValue<String, String>>() {
                @Override
                public Transformer<String, String, KeyValue<String, String>> get() {
                    return new NpsEscalationProcessorImpl(fraudAlertsTopic, properties);
                }
            }, STATE_STORE_NAME)
            // Filter out null values (non-escalation events)
            .filter((phone, alert) -> alert != null)
            // Write to fraud alerts topic
            .to(fraudAlertsTopic, org.apache.kafka.streams.kstream.Produced.with(Serdes.String(), Serdes.String()));
    }

    /**
     * Custom Processor implementation with KeyValueStore for NPS state tracking.
     */
    private static class NpsEscalationProcessorImpl implements Transformer<String, String, KeyValue<String, String>> {
        private static final Logger log = LoggerFactory.getLogger(NpsEscalationProcessorImpl.class);
        
        private final String fraudAlertsTopic;
        private final FraudDetectorProperties properties;
        private ProcessorContext context;
        private KeyValueStore<String, String> store;
        private final ObjectMapper objectMapper = new ObjectMapper();

        NpsEscalationProcessorImpl(String fraudAlertsTopic, FraudDetectorProperties properties) {
            this.fraudAlertsTopic = fraudAlertsTopic;
            this.properties = properties;
        }

        @Override
        @SuppressWarnings("unchecked")
        public void init(ProcessorContext context) {
            this.context = context;
            this.context.schedule(
                Duration.ofMinutes(1),
                PunctuationType.WALL_CLOCK_TIME,
                timestamp -> purgeExpiredEntries()
            );
            this.store = (KeyValueStore<String, String>) context.getStateStore(STATE_STORE_NAME);
            log.info("NpsEscalationTransformer initialized with StateStore: {}", STATE_STORE_NAME);
        }

        @Override
        @SuppressWarnings("unchecked")
        public KeyValue<String, String> transform(String phone, String jsonEvent) {
            try {
                JsonNode node = objectMapper.readTree(jsonEvent);
                JsonNode npsNode = node.get("npsScore");
                Integer npsScore = npsNode != null && !npsNode.isNull() ? npsNode.asInt() : null;
                
                if (npsScore == null || npsScore >= 2) {
                    return null;
                }

                String stateJson = store.get(phone);
                NpsState state;
                if (stateJson == null) {
                    state = new NpsState(0, System.currentTimeMillis());
                } else {
                    state = deserializeState(stateJson);
                }

                state.negativeCount++;
                state.lastActivity = System.currentTimeMillis();
                store.put(phone, serializeState(state));

                if (state.negativeCount >= properties.getNpsEscalationThreshold()) {
                    log.info("NPS escalation detected for phone: {}, count: {}", phone, state.negativeCount);

                    ObjectNode alert = objectMapper.createObjectNode();
                    alert.put("callId", "nps-" + phone + "-" + System.currentTimeMillis());
                    alert.put("phone", phone);
                    alert.put("pattern", "NPS_ESCALATION");
                    alert.put("count", state.negativeCount);
                    alert.put("timestamp", Instant.now().toString());
                    alert.put("severity", "HIGH");

                    store.delete(phone);
                    return KeyValue.pair(phone, objectMapper.writeValueAsString(alert));
                }
                return null;
            } catch (Exception e) {
                log.error("Error processing event: {}", e.getMessage());
                return null;
            }
        }

        /**
         * Purge expired entries (older than 24 hours).
         */
        private void purgeExpiredEntries() {
            long cutoff = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(24);
            store.all().forEachRemaining(entry -> {
                if (entry.value != null) {
                    NpsState state = deserializeState(entry.value);
                    if (state.lastActivity < cutoff) {
                        store.delete(entry.key);
                        log.debug("Purged expired NPS state for phone: {}", entry.key);
                    }
                }
            });
        }

        private String serializeState(NpsState state) {
            return state.negativeCount + "," + state.lastActivity;
        }

        private NpsState deserializeState(String json) {
            String[] parts = json.split(",");
            if (parts.length != 2) {
                return new NpsState(0, System.currentTimeMillis());
            }
            return new NpsState(Integer.parseInt(parts[0]), Long.parseLong(parts[1]));
        }

        @Override
        public void close() {
            // Store is managed by Kafka Streams
        }

    }

    /**
     * State class for NPS tracking per phone number.
     */
    private static class NpsState implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        int negativeCount;
        long lastActivity;

        NpsState(int negativeCount, long lastActivity) {
            this.negativeCount = negativeCount;
            this.lastActivity = lastActivity;
        }
    }
}
