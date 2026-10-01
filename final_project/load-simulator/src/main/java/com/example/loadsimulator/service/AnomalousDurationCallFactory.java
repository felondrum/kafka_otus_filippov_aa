package com.example.loadsimulator.service;

import com.example.loadsimulator.dto.CallEventRequest;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Generates call events with anomalous duration (301-600 seconds).
 */
public class AnomalousDurationCallFactory {

    private static final int MIN_DURATION = 301;
    private static final int MAX_DURATION = 600;

    private final PhoneGenerator phoneGenerator;
    private final ThreadLocalRandom random = ThreadLocalRandom.current();

    public AnomalousDurationCallFactory(PhoneGenerator phoneGenerator) {
        this.phoneGenerator = phoneGenerator;
    }

    public CallEventRequest generate(int callIndex) {
        CallEventRequest event = new CallEventRequest();
        event.setCallId(UUID.randomUUID().toString());
        event.setPhone(phoneGenerator.generateRandomPhone());
        event.setDuration(MIN_DURATION + random.nextInt(MAX_DURATION - MIN_DURATION + 1));
        event.setAgentId("agent-" + random.nextInt(100));
        event.setNpsScore(random.nextInt(11)); // 0-10
        return event;
    }
}
