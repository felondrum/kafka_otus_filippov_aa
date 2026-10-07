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
import java.time.ZoneId;
import java.util.Optional;
import java.util.UUID;

/**
 * Consumes completed call events from Kafka and enriches CallMetadata records
 * with agentId, customerPhone, and other fields that are not available in calls.metadata.
 */
@Component
public class CompletedEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(CompletedEventConsumer.class);

    private final CallMetadataRepository callMetadataRepository;
    private final ReportService reportService;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public CompletedEventConsumer(CallMetadataRepository callMetadataRepository,
                                  ReportService reportService) {
        this.callMetadataRepository = callMetadataRepository;
        this.reportService = reportService;
    }

    @KafkaListener(topics = "calls.completed", groupId = "reporting-nps",
            containerFactory = "kafkaListenerContainerFactory")
    public void consume(String jsonEvent, Acknowledgment ack) {
        try {
            JsonNode node = objectMapper.readTree(jsonEvent);

            // Handle Confluent JSON format: {"schema": ..., "payload": ...}
            if (node.has("payload")) {
                node = node.get("payload");
            }

            String callIdStr = node.has("callId") ? node.get("callId").asText() : null;
            String agentId = node.has("agentId") ? node.get("agentId").asText() : null;
            String phone = node.has("phone") ? node.get("phone").asText() : null;
            Integer duration = node.has("duration") && !node.get("duration").isNull()
                    ? node.get("duration").asInt() : null;
            Integer npsScore = node.has("npsScore") && !node.get("npsScore").isNull()
                    ? node.get("npsScore").asInt() : null;
            String status = node.has("status") ? node.get("status").asText() : "COMPLETED";
            String timestamp = node.has("timestamp") ? node.get("timestamp").asText() : null;

            if (callIdStr == null || callIdStr.isEmpty()) {
                log.warn("Received completed event without callId, skipping");
                ack.acknowledge();
                return;
            }

            UUID callId;
            try {
                callId = UUID.fromString(callIdStr);
            } catch (IllegalArgumentException e) {
                log.warn("Invalid UUID format for callId: {}, skipping", callIdStr);
                ack.acknowledge();
                return;
            }

            Optional<CallMetadata> existingMetadata = callMetadataRepository.findByCallId(callId);
            if (existingMetadata.isPresent()) {
                CallMetadata metadata = existingMetadata.get();
                boolean updated = false;

                // Enrich agentId if not already set
                if ((metadata.getAgentId() == null || metadata.getAgentId().equals("unknown")) && agentId != null) {
                    metadata.setAgentId(agentId);
                    updated = true;
                }

                // Enrich customerPhone if not already set
                if ((metadata.getCustomerPhone() == null || metadata.getCustomerPhone().equals("unknown")) && phone != null) {
                    metadata.setCustomerPhone(phone);
                    updated = true;
                }

                // Set duration if available
                if (metadata.getCallDuration() == null && duration != null) {
                    metadata.setCallDuration(duration);
                    updated = true;
                }

                // Update status to COMPLETED
                if (!"COMPLETED".equals(metadata.getCallStatus())) {
                    metadata.setCallStatus("COMPLETED");
                    updated = true;
                }

                // Parse timestamp and set call start time if not set
                if (metadata.getCallStartTime() == null && timestamp != null) {
                    try {
                        // Handle ISO 8601 timestamp (e.g., "2024-01-15T10:30:00Z")
                        LocalDateTime callTime = LocalDateTime.parse(timestamp.replace("Z", "+00:00").replaceAll("\\+00:00$", ""));
                        metadata.setCallStartTime(callTime);
                        updated = true;
                    } catch (Exception e) {
                        log.warn("Failed to parse timestamp '{}': {}", timestamp, e.getMessage());
                    }
                }

                if (updated) {
                    callMetadataRepository.save(metadata);
                    log.info("Enriched metadata for callId={}, agentId={}, phone={}", callId, agentId, phone);
                }
            } else {
                // Create a new record with full data from completed event
                CallMetadata newMetadata = new CallMetadata();
                newMetadata.setCallId(callId);
                newMetadata.setAgentId(agentId != null ? agentId : "unknown");
                newMetadata.setCustomerPhone(phone != null ? phone : "unknown");
                newMetadata.setCallStatus("COMPLETED");
                newMetadata.setCallDuration(duration);

                if (timestamp != null) {
                    try {
                        LocalDateTime callTime = LocalDateTime.parse(timestamp.replace("Z", "+00:00").replaceAll("\\+00:00$", ""));
                        newMetadata.setCallStartTime(callTime);
                    } catch (Exception e) {
                        log.warn("Failed to parse timestamp '{}', using now: {}", timestamp, e.getMessage());
                        newMetadata.setCallStartTime(LocalDateTime.now());
                    }
                } else {
                    newMetadata.setCallStartTime(LocalDateTime.now());
                }

                callMetadataRepository.save(newMetadata);
                log.info("Created enriched metadata record for callId={}, agentId={}, phone={}", callId, agentId, phone);
            }

            // Invalidate caches
            reportService.evictMetadataCache(callIdStr);
            reportService.evictReportCache();

            ack.acknowledge();
        } catch (Exception e) {
            log.error("Failed to process completed event: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to process completed event", e);
        }
    }
}
