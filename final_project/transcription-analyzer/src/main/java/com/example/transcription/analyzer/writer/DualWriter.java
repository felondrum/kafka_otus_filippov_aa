package com.example.transcription.analyzer.writer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Component
public class DualWriter {

    private static final Logger log = LoggerFactory.getLogger(DualWriter.class);
    public static final int MAX_RETRIES = 3;
    public static final long INITIAL_BACKOFF_MS = 1000;

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    public DualWriter(KafkaTemplate<String, String> kafkaTemplate,
                      JdbcTemplate jdbcTemplate,
                      ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
    }

    /**
     * Write enriched transcription to both Kafka and PostgreSQL.
     * Kafka produce happens before PostgreSQL write.
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

        // Ensure call_metadata record exists (FK constraint for call_transcriptions)
        // MetadataManager may have already created it, so we use ON CONFLICT DO NOTHING
        updateCallMetadata((String) mutableEnriched.get("call_id"));

        // Step 1: Write to Kafka first
        try {
            writeKafka((String) mutableEnriched.get("call_id"), mutableEnriched);
        } catch (Exception e) {
            log.error("Kafka write failed for callId={}", mutableEnriched.get("call_id"), e);
            throw new RuntimeException("Kafka write failed for callId=" + mutableEnriched.get("call_id"), e);
        }

        // Step 2: Write to PostgreSQL with retry
        try {
            writePostgreSQL((String) mutableEnriched.get("call_id"), mutableEnriched);
        } catch (Exception e) {
            log.error("PostgreSQL write failed for callId={} after all retries",
                    mutableEnriched.get("call_id"), e);
            sendToDLQ((String) mutableEnriched.get("call_id"), mutableEnriched);
        }
    }

    /**
     * Ensure call_metadata record exists with the given UUID.
     * This ensures FK constraint with call_transcriptions is satisfied.
     * Uses ON CONFLICT DO NOTHING in case MetadataManager already created the record.
     */
    private void updateCallMetadata(String uuidCallId) {
        try {
            jdbcTemplate.update(
                    "INSERT INTO call_metadata (call_id, customer_phone, agent_id, call_start_time, call_status) " +
                    "VALUES (?, '+79991234567', 'agent-test', NOW(), 'COMPLETED') " +
                    "ON CONFLICT (call_id) DO NOTHING",
                    UUID.fromString(uuidCallId)
            );
            log.debug("Ensured call_metadata exists with UUID: {}", uuidCallId);
        } catch (Exception e) {
            log.error("Failed to ensure call_metadata with UUID {}: {}", uuidCallId, e.getMessage(), e);
            throw new RuntimeException("Failed to ensure call_metadata exists", e);
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
     * Write to PostgreSQL call_transcriptions table with retry logic.
     */
    private void writePostgreSQL(String callId, Map<String, Object> enriched) {
        Exception lastException = null;

        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            try {
                // Cast callId to UUID for PostgreSQL
                java.util.UUID uuidCallId = java.util.UUID.fromString(callId);
                jdbcTemplate.update(
                        "INSERT INTO call_transcriptions " +
                        "(call_id, transcription_text, sentiment, urgency, problem, solution, confidence, " +
                        "segment, risk_level, priority) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        uuidCallId,
                        enriched.get("transcription_text"),
                        enriched.get("sentiment"),
                        enriched.get("urgency"),
                        enriched.get("problem"),
                        enriched.get("solution"),
                        enriched.get("confidence"),
                        enriched.get("segment"),
                        enriched.get("risk_level"),
                        enriched.get("priority")
                );

                log.info("Written enriched transcription to PostgreSQL: callId={}", callId);
                return;
            } catch (Exception e) {
                lastException = e;
                log.warn("PostgreSQL write attempt {}/{} failed for callId={}", attempt, MAX_RETRIES, callId, e);

                if (attempt < MAX_RETRIES) {
                    try {
                        long backoffMs = INITIAL_BACKOFF_MS * (long) Math.pow(2, attempt - 1);
                        log.debug("Retrying in {} ms...", backoffMs);
                        Thread.sleep(backoffMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        log.error("Interrupted during retry backoff for callId={}", callId, ie);
                        break;
                    }
                }
            }
        }

        throw new RuntimeException(
                String.format("PostgreSQL write failed after %d attempts for callId=%s", MAX_RETRIES, callId),
                lastException);
    }

    /**
     * Send failed event to internal DLQ (logged for manual review).
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
