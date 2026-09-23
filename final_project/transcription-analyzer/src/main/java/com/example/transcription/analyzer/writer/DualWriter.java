package com.example.transcription.analyzer.writer;

import com.example.transcription.avro.EnrichedTranscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Component
public class DualWriter {

    private static final Logger log = LoggerFactory.getLogger(DualWriter.class);
    private static final int MAX_RETRIES = 3;
    private static final long INITIAL_BACKOFF_MS = 1000;

    private final KafkaTemplate<String, EnrichedTranscription> kafkaTemplate;
    private final JdbcTemplate jdbcTemplate;

    public DualWriter(KafkaTemplate<String, EnrichedTranscription> kafkaTemplate,
                      JdbcTemplate jdbcTemplate) {
        this.kafkaTemplate = kafkaTemplate;
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * Write enriched transcription to both Kafka and PostgreSQL.
     * Kafka produce happens before PostgreSQL write.
     */
    public void write(EnrichedTranscription enriched) {
        String callId = enriched.getCallId();

        // Step 1: Write to Kafka first
        try {
            writeKafka(callId, enriched);
        } catch (Exception e) {
            log.error("Kafka write failed for callId={}", callId, e);
            throw new RuntimeException("Kafka write failed for callId=" + callId, e);
        }

        // Step 2: Write to PostgreSQL with retry
        try {
            writePostgreSQL(callId, enriched);
        } catch (Exception e) {
            log.error("PostgreSQL write failed for callId={} after all retries", callId, e);
            sendToDLQ(callId, enriched);
        }
    }

    /**
     * Write to Kafka topic transcription.enriched.
     */
    private void writeKafka(String callId, EnrichedTranscription enriched) {
        CompletableFuture<SendResult<String, EnrichedTranscription>> future =
                kafkaTemplate.send("transcription.enriched", callId, enriched);

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
    }

    /**
     * Write to PostgreSQL call_transcriptions table with retry logic.
     */
    private void writePostgreSQL(String callId, EnrichedTranscription enriched) {
        Exception lastException = null;

        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            try {
                jdbcTemplate.update(
                        "INSERT INTO call_transcriptions " +
                        "(call_id, transcription_text, sentiment, urgency, problem, solution, confidence) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?)",
                        callId,
                        enriched.getTranscriptionText(),
                        enriched.getSentiment(),
                        enriched.getUrgency(),
                        enriched.getProblem(),
                        enriched.getSolution(),
                        enriched.getConfidence()
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
    private void sendToDLQ(String callId, EnrichedTranscription enriched) {
        log.error("DLQ: Failed enriched transcription for callId={}, problem={}, solution={}, sentiment={}, urgency={}, priority={}",
                callId, enriched.getProblem(), enriched.getSolution(),
                enriched.getSentiment(), enriched.getUrgency(), enriched.getPriority());
        // In production, this would send to a DLQ topic
        // kafkaTemplate.send("transcription.enriched.dlq", callId, enriched);
    }
}
