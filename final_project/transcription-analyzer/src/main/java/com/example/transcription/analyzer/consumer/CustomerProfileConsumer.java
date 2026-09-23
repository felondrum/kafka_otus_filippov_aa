package com.example.transcription.analyzer.consumer;

import com.example.transcription.analyzer.enrichment.CustomerProfileManager;
import com.example.transcription.avro.CustomerProfile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class CustomerProfileConsumer {

    private static final Logger log = LoggerFactory.getLogger(CustomerProfileConsumer.class);

    private final CustomerProfileManager customerProfileManager;

    public CustomerProfileConsumer(CustomerProfileManager customerProfileManager) {
        this.customerProfileManager = customerProfileManager;
    }

    @KafkaListener(topics = "customers.profile", groupId = "enrichment-processor")
    public void consume(CustomerProfile profile) {
        log.debug("Received customer profile update: phone={}, segment={}, riskLevel={}",
                profile.getPhone(), profile.getSegment(), profile.getRiskLevel());
        customerProfileManager.loadProfile(profile);
    }
}
