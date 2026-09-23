package com.example.reportingnps.consumer;

import com.example.reportingnps.entity.CallMetadata;
import com.example.reportingnps.repository.CallMetadataRepository;
import com.example.reportingnps.service.ReportService;
import com.example.reportingnps.service.SentimentService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.retry.backoff.BackOffPolicy;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.UUID;

@Component
public class MetadataConsumer {

    private static final Logger log = LoggerFactory.getLogger(MetadataConsumer.class);

    private final CallMetadataRepository callMetadataRepository;
    private final ReportService reportService;
    private final SentimentService sentimentService;
    private final BackOffPolicy retryPolicy;

    public MetadataConsumer(CallMetadataRepository callMetadataRepository,
                            ReportService reportService,
                            SentimentService sentimentService) {
        this.callMetadataRepository = callMetadataRepository;
        this.reportService = reportService;
        this.sentimentService = sentimentService;
        this.retryPolicy = new ExponentialBackOffPolicy();
    }

    @KafkaListener(topics = "calls.metadata", groupId = "reporting-nps",
            containerFactory = "kafkaListenerContainerFactory")
    public void consume(Map<String, Object> event, Acknowledgment ack) {
        try {
            CallMetadata metadata = convertToCallMetadata(event);
            if (metadata != null) {
                callMetadataRepository.save(metadata);
                // Invalidate relevant caches
                if (metadata.getCallId() != null) {
                    reportService.evictMetadataCache(metadata.getCallId().toString());
                }
                reportService.evictReportCache();
            }
            ack.acknowledge();
        } catch (Exception e) {
            log.error("Failed to process metadata event: {}", e.getMessage(), e);
            // On failure, do not acknowledge - event will be reprocessed
            throw e;
        }
    }

    private CallMetadata convertToCallMetadata(Map<String, Object> event) {
        CallMetadata metadata = new CallMetadata();

        if (event.containsKey("callId")) {
            metadata.setCallId(UUID.fromString(event.get("callId").toString()));
        }
        if (event.containsKey("customerPhone")) {
            metadata.setCustomerPhone(event.get("customerPhone").toString());
        }
        if (event.containsKey("agentId")) {
            metadata.setAgentId(event.get("agentId").toString());
        }
        if (event.containsKey("callStartTime")) {
            metadata.setCallStartTime(LocalDateTime.parse(event.get("callStartTime").toString()));
        }
        if (event.containsKey("callEndTime")) {
            metadata.setCallEndTime(LocalDateTime.parse(event.get("callEndTime").toString()));
        }
        if (event.containsKey("callDuration")) {
            metadata.setCallDuration(Integer.parseInt(event.get("callDuration").toString()));
        }
        if (event.containsKey("callStatus")) {
            metadata.setCallStatus(event.get("callStatus").toString());
        }
        if (event.containsKey("queueName")) {
            metadata.setQueueName(event.get("queueName").toString());
        }
        if (event.containsKey("ivrSelection")) {
            metadata.setIvrSelection(event.get("ivrSelection").toString());
        }
        if (event.containsKey("callPurpose")) {
            metadata.setCallPurpose(event.get("callPurpose").toString());
        }
        if (event.containsKey("sentimentScore")) {
            metadata.setSentimentScore(new java.math.BigDecimal(event.get("sentimentScore").toString()));
        }
        if (event.containsKey("sentiment")) {
            metadata.setSentiment(event.get("sentiment").toString());
        }
        if (event.containsKey("urgency")) {
            metadata.setUrgency(event.get("urgency").toString());
        }
        if (event.containsKey("problem")) {
            metadata.setProblem(event.get("problem").toString());
        }
        if (event.containsKey("solution")) {
            metadata.setSolution(event.get("solution").toString());
        }
        if (event.containsKey("confidence")) {
            metadata.setConfidence(Double.parseDouble(event.get("confidence").toString()));
        }
        if (event.containsKey("segment")) {
            metadata.setSegment(event.get("segment").toString());
        }
        if (event.containsKey("riskLevel")) {
            metadata.setRiskLevel(event.get("riskLevel").toString());
        }
        if (event.containsKey("priority")) {
            metadata.setPriority(event.get("priority").toString());
        }

        return metadata;
    }
}
