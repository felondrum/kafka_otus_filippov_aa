package com.example.reportingnps.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.example.reportingnps.entity.CallMetadata;
import com.example.reportingnps.entity.CallTranscription;
import com.example.reportingnps.repository.CallMetadataRepository;
import com.example.reportingnps.repository.CallTranscriptionRepository;
import com.example.reportingnps.service.ReportService;
import com.example.reportingnps.service.SentimentService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class EnrichedTranscriptionConsumer {

    private static final Logger log = LoggerFactory.getLogger(EnrichedTranscriptionConsumer.class);

    private final CallTranscriptionRepository transcriptionRepository;
    private final CallMetadataRepository metadataRepository;
    private final ReportService reportService;
    private final SentimentService sentimentService;
    private final ObjectMapper objectMapper = new ObjectMapper();

    // Buffer for orphan events (call_id not found in metadata)
    private final ConcurrentHashMap<UUID, String> orphanBuffer = new ConcurrentHashMap<>();

    public EnrichedTranscriptionConsumer(CallTranscriptionRepository transcriptionRepository,
                                         CallMetadataRepository metadataRepository,
                                         ReportService reportService,
                                         SentimentService sentimentService) {
        this.transcriptionRepository = transcriptionRepository;
        this.metadataRepository = metadataRepository;
        this.reportService = reportService;
        this.sentimentService = sentimentService;
    }

    @KafkaListener(topics = "transcription.enriched", groupId = "reporting-nps",
            containerFactory = "kafkaListenerContainerFactory")
    public void consume(String jsonEvent, Acknowledgment ack) {
        try {
            JsonNode node = objectMapper.readTree(jsonEvent);

            // Handle Confluent JSON format: {"schema": ..., "payload": ...}
            // If payload exists, extract it; otherwise use node as-is (plain JSON)
            if (node.has("payload")) {
                node = node.get("payload");
            }

            String callIdStr = node.has("callId") ? node.get("callId").asText() : null;
            
            if (callIdStr == null || callIdStr.isEmpty()) {
                log.warn("Received enriched transcription without callId, skipping");
                ack.acknowledge();
                return;
            }
            
            // Parse callId as UUID
            UUID callId;
            try {
                callId = UUID.fromString(callIdStr);
            } catch (IllegalArgumentException e) {
                log.warn("Invalid UUID format for callId: {}, skipping enriched transcription", callIdStr);
                ack.acknowledge();
                return;
            }

            // Check FK integrity - call must exist in metadata
            Optional<CallMetadata> existingMetadata = metadataRepository.findByCallId(callId);
            if (existingMetadata.isEmpty()) {
                // Buffer orphan event for retry
                bufferOrphanEvent(callId, jsonEvent);
                ack.acknowledge(); // Ack but don't process yet
                return;
            }

            CallTranscription transcription = convertToTranscription(node);
            transcription.setCallId(callId);

            // Upsert: update existing or create new
            Optional<CallTranscription> existing = transcriptionRepository.findByCallId(callId);
            if (existing.isPresent()) {
                transcription.setTranscriptionId(existing.get().getTranscriptionId());
                transcription.setCreatedAt(existing.get().getCreatedAt());
            }

            transcriptionRepository.save(transcription);

            // Also update metadata with segment, riskLevel, priority if present
            CallMetadata metadata = existingMetadata.get();
            if (node.has("segment")) {
                metadata.setSegment(node.get("segment").asText());
            }
            if (node.has("riskLevel")) {
                metadata.setRiskLevel(node.get("riskLevel").asText());
            }
            if (node.has("priority")) {
                metadata.setPriority(node.get("priority").asText());
            }
            metadataRepository.save(metadata);

            // Invalidate caches
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

    private void bufferOrphanEvent(UUID callId, String jsonEvent) {
        orphanBuffer.computeIfAbsent(callId, k -> jsonEvent);
        log.warn("Buffered orphan event for callId: {}. Total buffered: {}", callId, orphanBuffer.size());
        // In production, would implement retry logic with scheduled task
        // For MVP, we log and move on
    }

    private CallTranscription convertToTranscription(JsonNode node) {
        CallTranscription transcription = new CallTranscription();

        if (node.has("transcriptionText")) {
            transcription.setTranscriptionText(node.get("transcriptionText").asText());
        }
        if (node.has("language")) {
            transcription.setLanguage(node.get("language").asText());
        }
        if (node.has("confidence")) {
            transcription.setConfidenceScore(node.get("confidence").asDouble());
        }
        if (node.has("problem")) {
            transcription.setProblem(node.get("problem").asText());
        }
        if (node.has("solution")) {
            transcription.setSolution(node.get("solution").asText());
        }
        if (node.has("sentiment")) {
            transcription.setSentiment(node.get("sentiment").asText());
        }
        if (node.has("urgency")) {
            transcription.setUrgency(node.get("urgency").asText());
        }

        return transcription;
    }
}
