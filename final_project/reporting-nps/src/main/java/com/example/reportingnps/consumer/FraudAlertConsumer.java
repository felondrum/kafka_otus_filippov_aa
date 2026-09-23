package com.example.reportingnps.consumer;

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

import java.util.Map;
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

    // Buffer for fraud alerts with unknown callId
    private final ConcurrentHashMap<String, Map<String, Object>> unknownCallBuffer = new ConcurrentHashMap<>();

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
    public void consume(Map<String, Object> event, Acknowledgment ack) {
        try {
            String callId = event.get("callId") != null ? event.get("callId").toString() : null;
            String phone = event.get("phone") != null ? event.get("phone").toString() : null;
            String pattern = event.get("pattern") != null ? event.get("pattern").toString() : "UNKNOWN";
            String severity = event.get("severity") != null ? event.get("severity").toString() : "MEDIUM";

            String agentId = null;

            // Fraud correlation: lookup callId to get agentId
            if (callId != null) {
                try {
                    UUID uuidCallId = UUID.fromString(callId);
                    Optional<CallMetadata> metadata = metadataRepository.findByCallId(uuidCallId);
                    if (metadata.isPresent()) {
                        agentId = metadata.get().getAgentId();
                    } else {
                        // Unknown callId - buffer and retry
                        bufferUnknownCallId(callId, event);
                    }
                } catch (IllegalArgumentException e) {
                    // Not a valid UUID, skip correlation
                    log.warn("Invalid callId format: {}", callId);
                }
            }

            // Aggregate fraud stats
            FraudStats stats = fraudStatsRepository
                    .findByPhoneAndPatternAndSeverity(phone, pattern, severity)
                    .orElse(new FraudStats());

            stats.setPhone(phone);
            stats.setCallId(callId);
            stats.setPattern(pattern);
            stats.setSeverity(severity);
            stats.setCount(stats.getCount() == null ? 1 : stats.getCount() + 1);
            stats.setAgentId(agentId);

            fraudStatsRepository.save(stats);

            // Invalidate caches
            reportService.evictReportCache();
            if (agentId != null) {
                agentReportService.evictAgentReportCache(agentId);
            }

            ack.acknowledge();
        } catch (Exception e) {
            log.error("Failed to process fraud alert event: {}", e.getMessage(), e);
            throw e;
        }
    }

    private void bufferUnknownCallId(String callId, Map<String, Object> event) {
        unknownCallBuffer.computeIfAbsent(callId, k -> event);
        log.warn("Buffered fraud alert for unknown callId: {}. Total buffered: {}", callId, unknownCallBuffer.size());
        // In production, would implement retry logic with scheduled task
        // Store with agentId=null for now
    }
}
