package com.example.transcription.analyzer.consumer;

import com.example.transcription.analyzer.processor.CallMetadataProcessor;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class CallCompletedConsumer {

    private static final Logger log = LoggerFactory.getLogger(CallCompletedConsumer.class);

    private final CallMetadataProcessor callMetadataProcessor;
    private final ObjectMapper objectMapper;

    public CallCompletedConsumer(CallMetadataProcessor callMetadataProcessor,
                                 ObjectMapper objectMapper) {
        this.callMetadataProcessor = callMetadataProcessor;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "calls.completed", groupId = "transcription-analyzer", containerFactory = "callCompletedContainerFactory")
    public void consume(String jsonEvent) {
        try {
            JsonNode event = objectMapper.readTree(jsonEvent);
            String callId = event.get("callId").asText();
            double durationMinutes = event.has("duration") && !event.get("duration").isNull()
                    ? event.get("duration").asDouble() / 60.0
                    : 5.0 / 60.0;
            String phone = event.has("phone") && !event.get("phone").isNull()
                    ? event.get("phone").asText()
                    : null;
            String agentId = event.has("agentId") && !event.get("agentId").isNull()
                    ? event.get("agentId").asText()
                    : null;

            log.info("Received call.completed event: callId={}, duration={}min, phone={}, agentId={}", callId, durationMinutes, phone, agentId);
            callMetadataProcessor.processCall(callId, durationMinutes, phone, agentId);
        } catch (JsonProcessingException e) {
            log.error("Failed to process call.completed event: {}", e.getMessage(), e);
            throw new RuntimeException(e);
        } catch (Exception e) {
            log.error("Failed to process call.completed event: {}", e.getMessage(), e);
            throw e;
        }
    }
}
