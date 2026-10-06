package com.example.transcription.analyzer.writer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Writes enriched transcription data to Kafka topic transcription.enriched.
 * PostgreSQL writes are handled exclusively by Kafka Connect JDBC Sink connector.
 */
@Component
public class DualWriter {

    private static final Logger log = LoggerFactory.getLogger(DualWriter.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public DualWriter(KafkaTemplate<String, String> kafkaTemplate,
                      ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    /**
     * Write enriched transcription to Kafka topic transcription.enriched.
     * PostgreSQL synchronization is handled by Kafka Connect JDBC Sink connector.
     */
    public void write(Map<String, Object> enriched) {
        String callId = (String) enriched.get("callId");

        // Create mutable copy with snake_case keys matching PostgreSQL table columns
        Map<String, Object> mutableEnriched = new LinkedHashMap<>();
        mutableEnriched.put("call_id", ensureValidUuid(callId));
        mutableEnriched.put("transcription_text", enriched.get("transcriptionText"));
        mutableEnriched.put("sentiment", enriched.get("sentiment"));
        mutableEnriched.put("urgency", enriched.get("urgency"));
        mutableEnriched.put("problem", enriched.get("problem"));
        mutableEnriched.put("solution", enriched.get("solution"));
        mutableEnriched.put("confidence", enriched.get("confidence"));
        mutableEnriched.put("segment", enriched.get("segment"));
        mutableEnriched.put("risk_level", enriched.get("riskLevel"));
        mutableEnriched.put("priority", enriched.get("priority"));

        // Write to Kafka — PostgreSQL is populated by Kafka Connect JDBC Sink
        try {
            writeKafka((String) mutableEnriched.get("call_id"), mutableEnriched);
        } catch (Exception e) {
            log.error("Kafka write failed for callId={}", mutableEnriched.get("call_id"), e);
            sendToDLQ((String) mutableEnriched.get("call_id"), mutableEnriched);
            throw new RuntimeException("Kafka write failed for callId=" + mutableEnriched.get("call_id"), e);
        }
    }

    /**
     * Ensure callId is a valid UUID. Generate new UUID if invalid.
     */
    private String ensureValidUuid(String callId) {
        try {
            UUID.fromString(callId);
            return callId;
        } catch (IllegalArgumentException e) {
            String newUuid = UUID.randomUUID().toString();
            log.warn("callId '{}' is not a valid UUID, generating new UUID: {}", callId, newUuid);
            return newUuid;
        }
    }

    /**
     * Write to Kafka topic transcription.enriched.
     * Produces Confluent JSON format: {"schema": ..., "payload": ...}
     * Required for Confluent JDBC Sink Connector with schemas.enable=true.
     */
    private void writeKafka(String callId, Map<String, Object> enriched) {
        try {
            // Build Confluent JSON format with schema and payload
            Map<String, Object> confluentJson = buildConfluentJson(enriched);
            String json = objectMapper.writeValueAsString(confluentJson);
            CompletableFuture<SendResult<String, String>> future =
                    kafkaTemplate.send("transcription.enriched", callId, json);

            future.whenComplete((result, ex) -> {
                if (ex == null) {
                    log.info("Produced enriched transcription to Kafka: callId={}, topic={}, partition={}, offset={}",
                            callId,
                            result.getRecordMetadata().topic(),
                            result.getRecordMetadata().partition(),
                            result.getRecordMetadata().offset());
                } else {
                    log.error("Failed to produce enriched transcription to Kafka: callId={}", callId, ex);
                    throw new RuntimeException("Kafka write failed", ex);
                }
            }).join();
        } catch (Exception e) {
            log.error("Failed to serialize enriched transcription: callId={}", callId, e);
            throw new RuntimeException("Serialization failed", e);
        }
    }

    /**
     * Build Confluent JSON format with Avro schema and payload.
     * The schema describes the struct fields for Confluent JDBC Sink Connector.
     */
    private Map<String, Object> buildConfluentJson(Map<String, Object> enriched) {
        // Build Avro schema for the enriched transcription struct
        List<Map<String, Object>> fields = new ArrayList<>();
        fields.add(makeField("call_id", "string", false));
        fields.add(makeField("transcription_text", "string", true));
        fields.add(makeField("sentiment", "string", true));
        fields.add(makeField("urgency", "string", true));
        fields.add(makeField("problem", "string", true));
        fields.add(makeField("solution", "string", true));
        fields.add(makeField("confidence", "float", true));
        fields.add(makeField("segment", "string", true));
        fields.add(makeField("risk_level", "string", true));
        fields.add(makeField("priority", "string", true));

        Map<String, Object> schema = new LinkedHashMap<>();
        schema.put("type", "struct");
        schema.put("name", "transcription.enriched.Value");
        schema.put("fields", fields);
        schema.put("optional", false);

        // Payload is the enriched map as LinkedHashMap to preserve field order
        Map<String, Object> payload = new LinkedHashMap<>(enriched);

        Map<String, Object> confluentJson = new LinkedHashMap<>();
        confluentJson.put("schema", schema);
        confluentJson.put("payload", payload);
        return confluentJson;
    }

    private Map<String, Object> makeField(String name, String type, boolean optional) {
        Map<String, Object> field = new LinkedHashMap<>();
        field.put("type", type);
        field.put("optional", optional);
        field.put("field", name);
        return field;
    }

    /**
     * Send failed event to DLQ topic for manual review.
     */
    private void sendToDLQ(String callId, Map<String, Object> enriched) {
        log.error("DLQ: Failed enriched transcription for callId={}, problem={}, solution={}, sentiment={}, urgency={}, risk_level={}, priority={}",
                callId, enriched.get("problem"), enriched.get("solution"),
                enriched.get("sentiment"), enriched.get("urgency"), enriched.get("risk_level"), enriched.get("priority"));
        try {
            String json = objectMapper.writeValueAsString(enriched);
            kafkaTemplate.send("transcription.enriched.dlq", callId, json).whenComplete((result, ex) -> {
                if (ex != null) {
                    log.error("DLQ send failed for callId={}", callId, ex);
                } else {
                    log.info("Event sent to DLQ: callId={}, topic={}, partition={}, offset={}",
                            callId,
                            result.getRecordMetadata().topic(),
                            result.getRecordMetadata().partition(),
                            result.getRecordMetadata().offset());
                }
            });
        } catch (Exception e) {
            log.error("Failed to send to DLQ for callId={}", callId, e);
        }
    }
}
