package com.example.callprocessor.service;

import com.example.callprocessor.avro.CallEvent;
import com.example.callprocessor.dto.CallEventRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Service
public class CallEventService {

    private static final Logger log = LoggerFactory.getLogger(CallEventService.class);

    private final KafkaTemplate<String, CallEvent> kafkaTemplate;
    private final TopicManager topicManager;
    private final AvroRecordConverter avroRecordConverter;
    private final String completedTopic;
    private final String metadataTopic;
    private final String dlqTopic;

    public CallEventService(KafkaTemplate<String, CallEvent> kafkaTemplate,
                            TopicManager topicManager,
                            AvroRecordConverter avroRecordConverter,
                            @org.springframework.beans.factory.annotation.Value("${call-processor.topics.completed:calls.completed}") String completedTopic,
                            @org.springframework.beans.factory.annotation.Value("${call-processor.topics.metadata:calls.metadata}") String metadataTopic,
                            @org.springframework.beans.factory.annotation.Value("${call-processor.topics.dlq:calls.dlq}") String dlqTopic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topicManager = topicManager;
        this.avroRecordConverter = avroRecordConverter;
        this.completedTopic = completedTopic;
        this.metadataTopic = metadataTopic;
        this.dlqTopic = dlqTopic;
    }

    @Retryable(
            retryFor = {RuntimeException.class},
            maxAttemptsExpression = "${call-processor.retry.max-attempts:3}",
            backoff = @Backoff(delayExpression = "${call-processor.retry.backoff-delay:1000}", multiplier = 2)
    )
    public void processCallEvent(CallEventRequest request, String correlationId) {
        log.info("Processing call event: callId={}, correlationId={}", request.getCallId(), correlationId);

        // Convert DTO to Avro record for calls.completed
        CallEvent completedRecord = avroRecordConverter.toCompletedRecord(request);
        produceToTopic(completedRecord, completedTopic, request.getCallId(), correlationId);

        // Convert DTO to Avro record for calls.metadata (with PENDING status)
        CallEvent metadataRecord = avroRecordConverter.toMetadataRecord(request);
        produceToTopic(metadataRecord, metadataTopic, request.getCallId(), correlationId);

        log.info("Call event processed successfully: callId={}, correlationId={}", request.getCallId(), correlationId);
    }

    @Retryable(
            retryFor = {RuntimeException.class},
            maxAttemptsExpression = "${call-processor.retry.max-attempts:3}",
            backoff = @Backoff(delayExpression = "${call-processor.retry.backoff-delay:1000}", multiplier = 2)
    )
    public void produceToTopic(CallEvent event, String topic, String key, String correlationId) {
        log.info("Producing event to topic={}, callId={}, correlationId={}", topic, key, correlationId);

        CompletableFuture<SendResult<String, CallEvent>> future = kafkaTemplate.send(topic, key, event);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to produce event to topic={}, callId={}, correlationId={}: {}",
                        topic, key, correlationId, ex.getMessage());
                throw new RuntimeException("Failed to produce event to " + topic, ex);
            } else {
                log.info("Event produced successfully to topic={}, partition={}, offset={}",
                        topic, result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
            }
        });

        try {
            future.get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.error("Error waiting for produce result: topic={}, callId={}, correlationId={}: {}",
                    topic, key, correlationId, e.getMessage());
            throw new RuntimeException("Error producing event", e);
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
