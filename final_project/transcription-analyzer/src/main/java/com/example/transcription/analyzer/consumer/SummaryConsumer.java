package com.example.transcription.analyzer.consumer;

import com.example.transcription.analyzer.generator.SummaryGenerator;
import com.example.transcription.analyzer.generator.SummaryGenerator.SummaryResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Component
public class SummaryConsumer {

    private static final Logger log = LoggerFactory.getLogger(SummaryConsumer.class);

    private final SummaryGenerator summaryGenerator;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public SummaryConsumer(SummaryGenerator summaryGenerator,
                           KafkaTemplate<String, String> kafkaTemplate,
                           ObjectMapper objectMapper) {
        this.summaryGenerator = summaryGenerator;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "transcription.raw", groupId = "summary-processor")
    public void consume(String jsonEvent) {
        try {
            JsonNode event = objectMapper.readTree(jsonEvent);
            String callId = event.get("callId").asText();
            String text = event.get("text").asText();

            log.info("Processing raw transcription for callId={}", callId);

            SummaryResult summary = summaryGenerator.generate(callId, text);

            Map<String, Object> summaryEvent = Map.of(
                    "callId", callId,
                    "problem", summary.getProblem(),
                    "solution", summary.getSolution(),
                    "sentiment", summary.getSentiment(),
                    "urgency", summary.getUrgency(),
                    "confidence", summary.getConfidence()
            );

            CompletableFuture<SendResult<String, String>> future =
                    kafkaTemplate.send("transcription.summary", callId, objectMapper.writeValueAsString(summaryEvent));

            future.whenComplete((result, ex) -> {
                if (ex == null) {
                    log.info("Produced transcription summary: callId={}, topic={}, partition={}, offset={}",
                            callId,
                            result.getRecordMetadata().topic(),
                            result.getRecordMetadata().partition(),
                            result.getRecordMetadata().offset());
                } else {
                    log.error("Failed to produce transcription summary: callId={}", callId, ex);
                }
            });
        } catch (JsonProcessingException e) {
            log.error("Error processing raw transcription: {}", e.getMessage(), e);
            throw new RuntimeException(e);
        } catch (Exception e) {
            log.error("Error processing raw transcription: {}", e.getMessage(), e);
        }
    }
}
