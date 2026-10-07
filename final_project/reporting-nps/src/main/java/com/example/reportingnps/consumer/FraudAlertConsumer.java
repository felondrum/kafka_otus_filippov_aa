package com.example.reportingnps.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.example.reportingnps.entity.CallMetadata;
import com.example.reportingnps.entity.FraudStats;
import com.example.reportingnps.repository.CallMetadataRepository;
import com.example.reportingnps.repository.FraudStatsRepository;
import com.example.reportingnps.service.AgentReportService;
import com.example.reportingnps.service.ReportService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class FraudAlertConsumer {

    private static final Logger log = LoggerFactory.getLogger(FraudAlertConsumer.class);

    private final FraudStatsRepository fraudStatsRepository;
    private final CallMetadataRepository metadataRepository;
    private final ReportService reportService;
    private final AgentReportService agentReportService;
    private final ObjectMapper objectMapper = new ObjectMapper();

    // Buffer for fraud alerts with unknown callId
    private final ConcurrentHashMap<String, String> unknownCallBuffer = new ConcurrentHashMap<>();

    public FraudAlertConsumer(FraudStatsRepository fraudStatsRepository,
                              CallMetadataRepository metadataRepository,
                              ReportService reportService,
                              AgentReportService agentReportService) {
        this.fraudStatsRepository = fraudStatsRepository;
        this.metadataRepository = metadataRepository;
        this.reportService = reportService;
        this.agentReportService = agentReportService;
    }

    @KafkaListener(topics = "calls.fraud-alerts", groupId = "reporting-nps",
            containerFactory = "kafkaListenerContainerFactory")
    public void consume(String jsonEvent, Acknowledgment ack) {
        try {
            JsonNode node = objectMapper.readTree(jsonEvent);
            
            String callIdStr = node.has("callId") ? node.get("callId").asText() : null;
            String phone = node.has("phone") ? node.get("phone").asText() : "unknown";
            String pattern = node.has("pattern") ? node.get("pattern").asText() : "UNKNOWN";
            String severity = node.has("severity") ? node.get("severity").asText() : "LOW";

            if (callIdStr == null || callIdStr.isEmpty()) {
                log.warn("Received fraud alert without callId, skipping");
                ack.acknowledge();
                return;
            }

            String agentId = null;

            // Fraud correlation: lookup callId to get agentId
            try {
                UUID uuidCallId = UUID.fromString(callIdStr);
                Optional<CallMetadata> metadata = metadataRepository.findByCallId(uuidCallId);
                if (metadata.isPresent()) {
                    agentId = metadata.get().getAgentId();
                    log.info("Fraud alert correlated: callId={}, agentId={}, pattern={}", callIdStr, agentId, pattern);
                } else {
                    // Unknown callId - buffer and retry
                    log.warn("Unknown callId in fraud alert: {}, buffering", callIdStr);
                    bufferUnknownCallId(callIdStr, jsonEvent);
                }
            } catch (IllegalArgumentException e) {
                // Not a valid UUID, skip correlation but still process fraud alert
                log.info("Non-UUID callId in fraud alert (skipping correlation): callId={}, pattern={}", callIdStr, pattern);
            }

            // Aggregate fraud stats
            FraudStats stats = fraudStatsRepository
                    .findByPhoneAndPatternAndSeverity(phone, pattern, severity)
                    .orElse(new FraudStats());

            stats.setPhone(phone);
            stats.setCallId(callIdStr);
            stats.setPattern(pattern);
            stats.setSeverity(severity);
            stats.setCount(stats.getCount() == null ? 1 : stats.getCount() + 1);
            stats.setAgentId(agentId);

            fraudStatsRepository.save(stats);
            log.info("Fraud stats updated: phone={}, pattern={}, severity={}, count={}", 
                    phone, pattern, severity, stats.getCount());

            // Invalidate caches
            reportService.evictReportCache();
            if (agentId != null) {
                agentReportService.evictAgentReportCache(agentId);
            }

            ack.acknowledge();
        } catch (JsonProcessingException e) {
            log.error("Failed to process fraud alert event: {}", e.getMessage(), e);
            throw new RuntimeException(e);
        } catch (Exception e) {
            log.error("Failed to process fraud alert event: {}", e.getMessage(), e);
            throw e;
        }
    }

    private void bufferUnknownCallId(String callId, String jsonEvent) {
        unknownCallBuffer.computeIfAbsent(callId, k -> jsonEvent);
        log.warn("Buffered fraud alert for unknown callId: {}. Total buffered: {}", callId, unknownCallBuffer.size());
        // In production, would implement retry logic with scheduled task
        // Store with agentId=null for now
    }
}
