package com.example.callprocessor.service;

import com.example.callprocessor.dto.CallEventRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Service
public class CallEventService {

    private static final Logger log = LoggerFactory.getLogger(CallEventService.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String completedTopic;
    private final String metadataTopic;

    public CallEventService(KafkaTemplate<String, String> kafkaTemplate,
                            ObjectMapper objectMapper,
                            @org.springframework.beans.factory.annotation.Value("${call-processor.topics.completed:calls.completed}") String completedTopic,
                            @org.springframework.beans.factory.annotation.Value("${call-processor.topics.metadata:calls.metadata}") String metadataTopic) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.completedTopic = completedTopic;
        this.metadataTopic = metadataTopic;
    }

    @Retryable(
            retryFor = {RuntimeException.class},
            maxAttemptsExpression = "${call-processor.retry.max-attempts:3}",
            backoff = @Backoff(delayExpression = "${call-processor.retry.backoff-delay:1000}", multiplier = 2)
    )
    public void processCallEvent(CallEventRequest request, String correlationId) {
        log.info("Processing call event: callId={}, correlationId={}", request.getCallId(), correlationId);

        // Convert to JSON for calls.completed
        Map<String, Object> completedEvent = new java.util.HashMap<>();
        completedEvent.put("callId", request.getCallId());
        completedEvent.put("phone", request.getPhone());
        completedEvent.put("duration", request.getDuration());
        completedEvent.put("agentId", request.getAgentId());
        completedEvent.put("npsScore", request.getNpsScore());
        completedEvent.put("status", "COMPLETED");
        completedEvent.put("timestamp", Instant.now().toString());

        produceToTopic(completedEvent, completedTopic, request.getCallId(), correlationId);

        // Convert to JSON for calls.metadata (with PENDING status)
        Map<String, Object> metadataEvent = new java.util.HashMap<>();
        metadataEvent.put("callId", request.getCallId());
        metadataEvent.put("status", "PENDING");
        metadataEvent.put("timestamp", Instant.now().toEpochMilli());

        produceToTopic(metadataEvent, metadataTopic, request.getCallId(), correlationId);

        log.info("Call event processed successfully: callId={}, correlationId={}", request.getCallId(), correlationId);
    }

    @Retryable(
            retryFor = {RuntimeException.class},
            maxAttemptsExpression = "${call-processor.retry.max-attempts:3}",
            backoff = @Backoff(delayExpression = "${call-processor.retry.backoff-delay:1000}", multiplier = 2)
    )
    public void produceToTopic(Map<String, Object> event, String topic, String key, String correlationId) {
        log.info("Producing event to topic={}, callId={}, correlationId={}", topic, key, correlationId);

        try {
            String json = objectMapper.writeValueAsString(event);
            CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(topic, key, json);
            SendResult<String, String> result = future.get(10, TimeUnit.SECONDS);

            log.info("Event produced successfully to topic={}, partition={}, offset={}",
                    topic, result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
        } catch (Exception e) {
            log.error("Failed to produce event to topic={}, callId={}, correlationId={}: {}",
                    topic, key, correlationId, e.getMessage());
            throw new RuntimeException("Failed to produce event to " + topic, e);
        }
    }

    public boolean checkKafkaConnectivity() {
        try {
            return kafkaTemplate.getProducerFactory() != null;
        } catch (Exception e) {
            return false;
        }
    }
}
