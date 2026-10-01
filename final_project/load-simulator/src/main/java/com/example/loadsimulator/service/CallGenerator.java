package com.example.loadsimulator.service;

import com.example.loadsimulator.dto.CallEventRequest;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Main call generator that produces random call events.
 * Used for normal calls and as a fallback for all scenarios.
 */
public class CallGenerator {

    private final PhoneGenerator phoneGenerator;
    private final ThreadLocalRandom random = ThreadLocalRandom.current();

    public CallGenerator(PhoneGenerator phoneGenerator) {
        this.phoneGenerator = phoneGenerator;
    }

    /**
     * Generates a single random call event (normal scenario).
     */
    public CallEventRequest generateNormalCall() {
        CallEventRequest event = new CallEventRequest();
        event.setCallId(UUID.randomUUID().toString());
        event.setPhone(phoneGenerator.generateRandomPhone());
        event.setDuration(30 + random.nextInt(270)); // 30-300 seconds
        event.setAgentId("agent-" + random.nextInt(100));
        event.setNpsScore(random.nextInt(11)); // 0-10
        return event;
    }

    /**
     * Generates a batch of normal calls (for filling remaining calls after fraud bursts).
     */
    public List<CallEventRequest> generateBatch(int count) {
        List<CallEventRequest> batch = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            batch.add(generateNormalCall());
        }
        return batch;
    }
}
