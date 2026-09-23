package com.example.transcription.analyzer.processor;

import com.example.transcription.analyzer.enrichment.EnrichmentService;
import com.example.transcription.analyzer.generator.SummaryGenerator;
import com.example.transcription.analyzer.generator.SummaryGenerator.SummaryResult;
import com.example.transcription.analyzer.metadata.MetadataManager;
import com.example.transcription.analyzer.producer.TranscriptionProducer;
import com.example.transcription.analyzer.writer.DualWriter;
import com.example.transcription.avro.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

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
    private final KafkaTemplate<String, TranscriptionSummary> summaryKafkaTemplate;

    public CallMetadataProcessor(TranscriptionProducer transcriptionProducer,
                                 MetadataManager metadataManager,
                                 SummaryGenerator summaryGenerator,
                                 EnrichmentService enrichmentService,
                                 DualWriter dualWriter,
                                 KafkaTemplate<String, TranscriptionSummary> summaryKafkaTemplate) {
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

        // Step 1: Produce raw transcription and set TRANSCRIBING status
        metadataManager.onTranscriptionProduced(callId);
        RawTranscription rawTranscription = transcriptionProducer.produce(callId, durationMinutes);

        // Step 2: Generate summary (SUMMARIZING status)
        metadataManager.onSummaryStarted(callId);
        SummaryResult summary = summaryGenerator.generate(callId, rawTranscription.getText());

        // Step 3: Produce summary to Kafka
        TranscriptionSummary transcriptionSummary = new TranscriptionSummary(
                callId,
                summary.getProblem(),
                summary.getSolution(),
                summary.getSentiment(),
                summary.getUrgency(),
                summary.getConfidence()
        );

        CompletableFuture<SendResult<String, TranscriptionSummary>> future =
                summaryKafkaTemplate.send("transcription.summary", callId, transcriptionSummary);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Produced transcription summary: callId={}", callId);
            } else {
                log.error("Failed to produce transcription summary: callId={}", callId, ex);
            }
        });

        // Step 4: Enrich with customer profile
        EnrichedTranscription enriched = enrichmentService.enrich(
                transcriptionSummary,
                rawTranscription.getText(),
                phone
        );

        // Step 5: Dual-write (Kafka + PostgreSQL)
        dualWriter.write(enriched);

        // Step 6: Set COMPLETED status
        metadataManager.onDualWriteComplete(callId);

        log.info("Call processing complete: callId={}", callId);
    }
}
