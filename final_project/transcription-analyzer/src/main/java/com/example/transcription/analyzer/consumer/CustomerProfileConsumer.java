package com.example.transcription.analyzer.consumer;

import com.example.transcription.analyzer.enrichment.CustomerProfileManager;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class CustomerProfileConsumer {

    private static final Logger log = LoggerFactory.getLogger(CustomerProfileConsumer.class);

    private final CustomerProfileManager customerProfileManager;
    private final ObjectMapper objectMapper;

    public CustomerProfileConsumer(CustomerProfileManager customerProfileManager,
                                   ObjectMapper objectMapper) {
        this.customerProfileManager = customerProfileManager;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "customers.profile", groupId = "enrichment-processor")
    public void consume(String jsonEvent) {
        try {
            JsonNode event = objectMapper.readTree(jsonEvent);
            String phone = event.get("phone").asText();
            String segment = event.get("segment").asText();
            String riskLevel = event.get("riskLevel").asText();

            log.debug("Received customer profile update: phone={}, segment={}, riskLevel={}",
                    phone, segment, riskLevel);
            customerProfileManager.loadProfile(phone, segment, riskLevel);
        } catch (JsonProcessingException e) {
            log.error("Failed to process customer profile: {}", e.getMessage(), e);
            throw new RuntimeException(e);
        } catch (Exception e) {
            log.error("Failed to process customer profile: {}", e.getMessage(), e);
            throw e;
        }
    }
}
