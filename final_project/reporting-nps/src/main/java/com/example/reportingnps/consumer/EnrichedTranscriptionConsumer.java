package com.example.reportingnps.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.example.reportingnps.service.ReportService;
import com.example.reportingnps.service.SentimentService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * Reads enriched transcription events from Kafka for cache invalidation only.
 * Data is populated in PostgreSQL exclusively by Kafka Connect JDBC Sink connector.
 */
@Component
public class EnrichedTranscriptionConsumer {

    private static final Logger log = LoggerFactory.getLogger(EnrichedTranscriptionConsumer.class);

    private final ReportService reportService;
    private final SentimentService sentimentService;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public EnrichedTranscriptionConsumer(ReportService reportService,
                                         SentimentService sentimentService) {
        this.reportService = reportService;
        this.sentimentService = sentimentService;
    }

    @KafkaListener(topics = "transcription.enriched", groupId = "reporting-nps",
            containerFactory = "kafkaListenerContainerFactory")
    public void consume(String jsonEvent, Acknowledgment ack) {
        try {
            JsonNode node = objectMapper.readTree(jsonEvent);

            // Handle Confluent JSON format: {"schema": ..., "payload": ...}
            if (node.has("payload")) {
                node = node.get("payload");
            }

            String callIdStr = node.has("callId") ? node.get("callId").asText() : null;

            if (callIdStr == null || callIdStr.isEmpty()) {
                log.warn("Received enriched transcription without callId, skipping");
                ack.acknowledge();
                return;
            }

            // Validate callId format
            try {
                UUID.fromString(callIdStr);
            } catch (IllegalArgumentException e) {
                log.warn("Invalid UUID format for callId: {}, skipping enriched transcription", callIdStr);
                ack.acknowledge();
                return;
            }

            // Data is already in PostgreSQL via Kafka Connect — only invalidate caches
            log.debug("Received enriched transcription for callId={}, invalidating caches", callIdStr);
            reportService.evictMetadataCache(callIdStr);
            reportService.evictReportCache();
            sentimentService.evictSentimentCache();

            ack.acknowledge();
        } catch (JsonProcessingException e) {
            log.error("Failed to process enriched transcription event: {}", e.getMessage(), e);
            throw new RuntimeException(e);
        } catch (Exception e) {
            log.error("Failed to process enriched transcription event: {}", e.getMessage(), e);
            throw e;
        }
    }
}
