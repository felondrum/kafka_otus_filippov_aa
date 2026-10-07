package com.example.loadsimulator.service;

import com.example.loadsimulator.dto.CallEventRequest;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Generates bursts of calls from the same phone number (for FREQUENT_CALLS scenario).
 */
public class FrequentCallsCallFactory {

    private static final int MIN_BURST_SIZE = 6;
    private static final int MAX_BURST_SIZE = 10;

    private final PhoneGenerator phoneGenerator;
    private final ThreadLocalRandom random = ThreadLocalRandom.current();

    public FrequentCallsCallFactory(PhoneGenerator phoneGenerator) {
        this.phoneGenerator = phoneGenerator;
    }

    /**
     * Generates a burst of calls from the same phone number.
     */
    public List<CallEventRequest> generateBurst(int burstIndex) {
        int burstSize = MIN_BURST_SIZE + random.nextInt(MAX_BURST_SIZE - MIN_BURST_SIZE + 1);
        String phone = phoneGenerator.generateFrequentCallPhone();
        List<CallEventRequest> burst = new ArrayList<>();

        for (int i = 0; i < burstSize; i++) {
            CallEventRequest event = new CallEventRequest();
            event.setCallId(UUID.randomUUID().toString());
            event.setPhone(phone);
            event.setDuration(30 + random.nextInt(120)); // 30-150 seconds
            event.setAgentId("agent-" + random.nextInt(100));
            event.setNpsScore(random.nextInt(11));
            burst.add(event);
        }
        return burst;
    }
}
