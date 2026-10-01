package com.example.transcription.analyzer.processor;

import com.example.transcription.analyzer.enrichment.EnrichmentService;
import com.example.transcription.analyzer.generator.SummaryGenerator;
import com.example.transcription.analyzer.generator.SummaryGenerator.SummaryResult;
import com.example.transcription.analyzer.metadata.MetadataManager;
import com.example.transcription.analyzer.producer.TranscriptionProducer;
import com.example.transcription.analyzer.writer.DualWriter;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Component
public class CallMetadataProcessor {

    private static final Logger log = LoggerFactory.getLogger(CallMetadataProcessor.class);

    private final TranscriptionProducer transcriptionProducer;
    private final MetadataManager metadataManager;
    private final SummaryGenerator summaryGenerator;
    private final EnrichmentService enrichmentService;
    private final DualWriter dualWriter;
    private final KafkaTemplate<String, String> summaryKafkaTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public CallMetadataProcessor(TranscriptionProducer transcriptionProducer,
                                 MetadataManager metadataManager,
                                 SummaryGenerator summaryGenerator,
                                 EnrichmentService enrichmentService,
                                 DualWriter dualWriter,
                                 KafkaTemplate<String, String> summaryKafkaTemplate) {
        this.transcriptionProducer = transcriptionProducer;
        this.metadataManager = metadataManager;
        this.summaryGenerator = summaryGenerator;
        this.enrichmentService = enrichmentService;
        this.dualWriter = dualWriter;
        this.summaryKafkaTemplate = summaryKafkaTemplate;
    }

    /**
     * Process a completed call event.
     * This is the entry point when a call completes.
     *
     * @param callId unique call identifier
     * @param durationMinutes call duration in minutes
     * @param phone customer phone number (for profile lookup)
     */
    public void processCall(String callId, double durationMinutes, String phone) {
        log.info("Processing call: callId={}, duration={}min, phone={}", callId, durationMinutes, phone);

        // Generate a consistent UUID for this call (PostgreSQL requires UUID type)
        String resolvedCallId = ensureValidUuid(callId);
        if (!callId.equals(resolvedCallId)) {
            log.warn("Original callId '{}' is not a valid UUID, using: {}", callId, resolvedCallId);
        }

        try {
            // Step 1: Produce raw transcription and set TRANSCRIBING status
            metadataManager.onTranscriptionProduced(resolvedCallId);
            Map<String, Object> rawTranscription = transcriptionProducer.produce(callId, durationMinutes);

            // Step 2: Generate summary (SUMMARIZING status)
            metadataManager.onSummaryStarted(resolvedCallId);
            SummaryResult summary = summaryGenerator.generate(callId, (String) rawTranscription.get("text"));

            // Step 3: Produce summary to Kafka (use resolved UUID as key)
            Map<String, Object> transcriptionSummary = new LinkedHashMap<>();
            transcriptionSummary.put("callId", resolvedCallId);
            transcriptionSummary.put("problem", summary.getProblem());
            transcriptionSummary.put("solution", summary.getSolution());
            transcriptionSummary.put("sentiment", summary.getSentiment());
            transcriptionSummary.put("urgency", summary.getUrgency());
            transcriptionSummary.put("confidence", summary.getConfidence());

            String summaryJson = objectMapper.writeValueAsString(transcriptionSummary);
            CompletableFuture<SendResult<String, String>> future =
                    summaryKafkaTemplate.send("transcription.summary", resolvedCallId, summaryJson);

            future.whenComplete((result, ex) -> {
                if (ex == null) {
                    log.info("Produced transcription summary: callId={}", resolvedCallId);
                } else {
                    log.error("Failed to produce transcription summary: callId={}", resolvedCallId, ex);
                }
            });

            // Step 4: Enrich with customer profile
            Map<String, Object> enriched = enrichmentService.enrich(
                    transcriptionSummary,
                    (String) rawTranscription.get("text"),
                    phone
            );

            // Step 5: Dual-write (Kafka + PostgreSQL)
            dualWriter.write(enriched);

            // Step 6: Set COMPLETED status
            metadataManager.onDualWriteComplete(resolvedCallId);

            log.info("Call processing complete: callId={}", resolvedCallId);
        } catch (Exception e) {
            log.error("Error processing call: callId={}", callId, e);
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
            return UUID.randomUUID().toString();
        }
    }
}
