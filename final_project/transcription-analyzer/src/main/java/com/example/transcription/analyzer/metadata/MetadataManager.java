package com.example.transcription.analyzer.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Component
public class MetadataManager {

    private static final Logger log = LoggerFactory.getLogger(MetadataManager.class);

    public static final String STATUS_TRANSCRIBING = "TRANSCRIBING";
    public static final String STATUS_SUMMARIZING = "SUMMARIZING";
    public static final String STATUS_COMPLETED = "COMPLETED";

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public MetadataManager(KafkaTemplate<String, String> kafkaTemplate,
                           JdbcTemplate jdbcTemplate) {
        this.kafkaTemplate = kafkaTemplate;
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * Track status transitions for a call.
     * Expected sequence: TRANSCRIBING -> SUMMARIZING -> COMPLETED
     */
    public void transition(String callId, String status) {
        transition(callId, null, null, null, status);
    }

    /**
     * Initialize call_metadata record with all fields, or update status if record exists.
     */
    public void transition(String callId, String phone, String agentId, Double callDurationMinutes, String status) {
        // Ensure callId is a valid UUID (PostgreSQL call_metadata requires UUID type)
        String uuidCallId = ensureValidUuid(callId);

        // 1. Insert/update call_metadata in PostgreSQL
        try {
            if (phone != null && agentId != null) {
                // Initial insert with all fields
                jdbcTemplate.update(
                        "INSERT INTO call_metadata (call_id, customer_phone, agent_id, call_start_time, call_duration, call_status) " +
                        "VALUES (?, ?, ?, NOW(), ?, 'COMPLETED') " +
                        "ON CONFLICT (call_id) DO UPDATE SET " +
                        "updated_at = CURRENT_TIMESTAMP",
                        UUID.fromString(uuidCallId), phone, agentId,
                        callDurationMinutes != null ? (int) Math.round(callDurationMinutes * 60) : null
                );
            } else {
                // Status-only update — record must already exist (created by initialize())
                jdbcTemplate.update(
                        "UPDATE call_metadata SET call_status = 'COMPLETED', updated_at = CURRENT_TIMESTAMP " +
                        "WHERE call_id = ?",
                        UUID.fromString(uuidCallId)
                );
            }
        } catch (Exception e) {
            log.warn("Failed to update call_metadata for callId={}: {}", callId, e.getMessage());
        }

        // 2. Produce to Kafka calls.metadata topic
        try {
            Map<String, Object> metadata = Map.of(
                    "callId", callId,
                    "status", status,
                    "timestamp", System.currentTimeMillis()
            );

            CompletableFuture<SendResult<String, String>> future =
                    kafkaTemplate.send("calls.metadata", callId, objectMapper.writeValueAsString(metadata));

            future.whenComplete((result, ex) -> {
                if (ex == null) {
                    log.info("Produced metadata event: callId={}, status={}, topic={}, partition={}, offset={}",
                            callId, status,
                            result.getRecordMetadata().topic(),
                            result.getRecordMetadata().partition(),
                            result.getRecordMetadata().offset());
                } else {
                    log.error("Failed to produce metadata event: callId={}, status={}", callId, status, ex);
                }
            });
        } catch (Exception e) {
            log.error("Failed to serialize metadata event: callId={}, status={}", callId, status, e);
        }
    }

    /**
     * Convenience methods for each status transition.
     */
    public void onTranscriptionProduced(String callId) {
        transition(callId, STATUS_TRANSCRIBING);
    }

    public void onSummaryStarted(String callId) {
        transition(callId, STATUS_SUMMARIZING);
    }

    public void onDualWriteComplete(String callId) {
        transition(callId, STATUS_COMPLETED);
    }

    /**
     * Initialize call_metadata with full data.
     */
    public void initialize(String callId, String phone, String agentId, Double callDurationMinutes) {
        transition(callId, phone, agentId, callDurationMinutes, STATUS_TRANSCRIBING);
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
     * Verify status transition sequence.
     * Returns true if the new status is valid in the sequence.
     */
    public static boolean isValidTransition(String current, String next) {
        if (next == null) return false;
        
        List<String> sequence = List.of(
                STATUS_TRANSCRIBING,
                STATUS_SUMMARIZING,
                STATUS_COMPLETED
        );

        int currentIndex = current != null ? sequence.indexOf(current) : -1;
        int nextIndex = sequence.indexOf(next);

        // Allow transitions to the next status in sequence
        return nextIndex == currentIndex + 1 || (nextIndex == 0 && currentIndex == -1);
    }
}
