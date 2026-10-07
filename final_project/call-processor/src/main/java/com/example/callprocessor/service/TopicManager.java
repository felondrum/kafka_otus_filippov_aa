package com.example.callprocessor.service;

import org.apache.kafka.clients.admin.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.ExecutionException;

@Component
public class TopicManager {

    private static final Logger log = LoggerFactory.getLogger(TopicManager.class);

    private final AdminClient adminClient;
    private final String completedTopic;
    private final String metadataTopic;
    private final String dlqTopic;

    public TopicManager(AdminClient adminClient,
                        @Value("${call-processor.topics.completed:calls.completed}") String completedTopic,
                        @Value("${call-processor.topics.metadata:calls.metadata}") String metadataTopic,
                        @Value("${call-processor.topics.dlq:calls.dlq}") String dlqTopic) {
        this.adminClient = adminClient;
        this.completedTopic = completedTopic;
        this.metadataTopic = metadataTopic;
        this.dlqTopic = dlqTopic;
    }

    @PostConstruct
    public void initTopics() {
        log.info("Initializing Kafka topics...");
        try {
            createTopicsIfNotExists();
            log.info("Topic initialization completed");
        } catch (Exception e) {
            log.warn("Failed to create topics (will retry on next startup): {}", e.getMessage());
        }
    }

    private void createTopicsIfNotExists() throws ExecutionException, InterruptedException {
        Set<String> existingTopics = adminClient.listTopics().names().get();

        List<NewTopic> topicsToCreate = new ArrayList<>();

        if (!existingTopics.contains(completedTopic)) {
            topicsToCreate.add(new NewTopic(completedTopic, 6, (short) 1)
                    .configs(Map.of(
                            "retention.ms", "604800000",
                            "compression.type", "lz4"
                    )));
            log.info("Creating topic: {}", completedTopic);
        } else {
            log.info("Topic already exists: {}", completedTopic);
        }

        if (!existingTopics.contains(metadataTopic)) {
            topicsToCreate.add(new NewTopic(metadataTopic, 6, (short) 1)
                    .configs(Map.of(
                            "cleanup.policy", "compact",
                            "retention.ms", "2592000000"
                    )));
            log.info("Creating topic: {} (compacted)", metadataTopic);
        } else {
            log.info("Topic already exists: {}", metadataTopic);
        }

        if (!existingTopics.contains(dlqTopic)) {
            topicsToCreate.add(new NewTopic(dlqTopic, 6, (short) 1)
                    .configs(Map.of(
                            "retention.ms", "2592000000",
                            "retention.bytes", "-1"
                    )));
            log.info("Creating topic: {}", dlqTopic);
        } else {
            log.info("Topic already exists: {}", dlqTopic);
        }

        if (!topicsToCreate.isEmpty()) {
            adminClient.createTopics(topicsToCreate).all().get();
            log.info("Successfully created {} topics", topicsToCreate.size());
        }
    }
}
