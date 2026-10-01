package com.example.reportingnps.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.example.reportingnps.entity.CallMetadata;
import com.example.reportingnps.repository.CallMetadataRepository;
import com.example.reportingnps.service.ReportService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.Optional;
import java.util.UUID;

@Component
public class MetadataConsumer {

    private static final Logger log = LoggerFactory.getLogger(MetadataConsumer.class);

    private final CallMetadataRepository callMetadataRepository;
    private final ReportService reportService;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public MetadataConsumer(CallMetadataRepository callMetadataRepository,
                            ReportService reportService) {
        this.callMetadataRepository = callMetadataRepository;
        this.reportService = reportService;
    }

    @KafkaListener(topics = "calls.metadata", groupId = "reporting-nps",
            containerFactory = "kafkaListenerContainerFactory")
    public void consume(String jsonEvent, Acknowledgment ack) {
        try {
            JsonNode node = objectMapper.readTree(jsonEvent);
            
            String callIdStr = node.has("callId") ? node.get("callId").asText() : null;
            String status = node.has("status") ? node.get("status").asText() : "PENDING";
            
            if (callIdStr == null || callIdStr.isEmpty()) {
                log.warn("Received metadata event without callId, skipping");
                ack.acknowledge();
                return;
            }
            
            // Try to parse as UUID
            UUID callId = null;
            try {
                callId = UUID.fromString(callIdStr);
            } catch (IllegalArgumentException e) {
                log.warn("Invalid UUID format for callId: {}, skipping metadata update", callIdStr);
                ack.acknowledge();
                return;
            }
            
            // Try to find existing record and update status, or create new
            Optional<CallMetadata> existingMetadata = callMetadataRepository.findByCallId(callId);
            if (existingMetadata.isPresent()) {
                CallMetadata metadata = existingMetadata.get();
                metadata.setCallStatus(status);
                callMetadataRepository.save(metadata);
                log.info("Updated metadata status for callId={}, status={}", callId, status);
            } else {
                // Create minimal metadata record - will be enriched later by enriched transcription
                CallMetadata newMetadata = new CallMetadata();
                newMetadata.setCallId(callId);
                newMetadata.setCallStatus(status);
                newMetadata.setAgentId("unknown");
                newMetadata.setCustomerPhone("unknown");
                newMetadata.setCallStartTime(LocalDateTime.now());
                callMetadataRepository.save(newMetadata);
                log.info("Created minimal metadata record for callId={}, status={}", callId, status);
            }
            
            // Invalidate relevant caches
            reportService.evictMetadataCache(callIdStr);
            reportService.evictReportCache();
            
            ack.acknowledge();
        } catch (Exception e) {
            log.error("Failed to process metadata event: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to process metadata event", e);
        }
    }
}
