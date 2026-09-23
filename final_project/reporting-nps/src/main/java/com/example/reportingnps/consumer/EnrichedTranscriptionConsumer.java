package com.example.reportingnps.consumer;

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

import java.time.LocalDateTime;
import java.util.Map;
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

    // Buffer for orphan events (call_id not found in metadata)
    private final ConcurrentHashMap<UUID, Map<String, Object>> orphanBuffer = new ConcurrentHashMap<>();

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
    public void consume(Map<String, Object> event, Acknowledgment ack) {
        try {
            UUID callId = UUID.fromString(event.get("callId").toString());

            // Check FK integrity - call must exist in metadata
            Optional<CallMetadata> existingMetadata = metadataRepository.findByCallId(callId);
            if (existingMetadata.isEmpty()) {
                // Buffer orphan event for retry
                bufferOrphanEvent(callId, event);
                ack.acknowledge(); // Ack but don't process yet
                return;
            }

            CallTranscription transcription = convertToTranscription(event);
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
            if (event.containsKey("segment")) {
                metadata.setSegment(event.get("segment").toString());
            }
            if (event.containsKey("riskLevel")) {
                metadata.setRiskLevel(event.get("riskLevel").toString());
            }
            if (event.containsKey("priority")) {
                metadata.setPriority(event.get("priority").toString());
            }
            metadataRepository.save(metadata);

            // Invalidate caches
            reportService.evictMetadataCache(callId.toString());
            reportService.evictReportCache();
            sentimentService.evictSentimentCache();

            ack.acknowledge();
        } catch (Exception e) {
            log.error("Failed to process enriched transcription event: {}", e.getMessage(), e);
            throw e;
        }
    }

    private void bufferOrphanEvent(UUID callId, Map<String, Object> event) {
        orphanBuffer.computeIfAbsent(callId, k -> event);
        log.warn("Buffered orphan event for callId: {}. Total buffered: {}", callId, orphanBuffer.size());
        // In production, would implement retry logic with scheduled task
        // For MVP, we log and move on
    }

    private CallTranscription convertToTranscription(Map<String, Object> event) {
        CallTranscription transcription = new CallTranscription();

        if (event.containsKey("callId")) {
            transcription.setCallId(UUID.fromString(event.get("callId").toString()));
        }
        if (event.containsKey("transcriptionText")) {
            transcription.setTranscriptionText(event.get("transcriptionText").toString());
        }
        if (event.containsKey("language")) {
            transcription.setLanguage(event.get("language").toString());
        }
        if (event.containsKey("confidenceScore")) {
            transcription.setConfidenceScore(Double.parseDouble(event.get("confidenceScore").toString()));
        }
        if (event.containsKey("problem")) {
            transcription.setProblem(event.get("problem").toString());
        }
        if (event.containsKey("solution")) {
            transcription.setSolution(event.get("solution").toString());
        }
        if (event.containsKey("sentiment")) {
            transcription.setSentiment(event.get("sentiment").toString());
        }
        if (event.containsKey("urgency")) {
            transcription.setUrgency(event.get("urgency").toString());
        }
        if (event.containsKey("confidence")) {
            transcription.setConfidence(Double.parseDouble(event.get("confidence").toString()));
        }
        if (event.containsKey("segment")) {
            transcription.setSegment(event.get("segment").toString());
        }
        if (event.containsKey("riskLevel")) {
            transcription.setRiskLevel(event.get("riskLevel").toString());
        }
        if (event.containsKey("priority")) {
            transcription.setPriority(event.get("priority").toString());
        }

        return transcription;
    }
}
