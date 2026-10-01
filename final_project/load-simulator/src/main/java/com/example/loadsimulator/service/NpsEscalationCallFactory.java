package com.example.loadsimulator.service;

import com.example.loadsimulator.dto.CallEventRequest;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Generates calls with low NPS scores for NPS_ESCALATION scenario.
 * Generates 3+ calls from the same phone with npsScore < 2.
 */
public class NpsEscalationCallFactory {

    private static final int MIN_NPS_CALLS = 3;
    private static final int MAX_NPS_CALLS = 5;

    private final PhoneGenerator phoneGenerator;
    private final ThreadLocalRandom random = ThreadLocalRandom.current();

    public NpsEscalationCallFactory(PhoneGenerator phoneGenerator) {
        this.phoneGenerator = phoneGenerator;
    }

    /**
     * Generates a sequence of calls with low NPS from the same phone.
     */
    public List<CallEventRequest> generateSequence(int sequenceIndex) {
        int sequenceSize = MIN_NPS_CALLS + random.nextInt(MAX_NPS_CALLS - MIN_NPS_CALLS + 1);
        String phone = phoneGenerator.generateNpsEscalationPhone();
        List<CallEventRequest> sequence = new ArrayList<>();

        for (int i = 0; i < sequenceSize; i++) {
            CallEventRequest event = new CallEventRequest();
            event.setCallId(UUID.randomUUID().toString());
            event.setPhone(phone);
            event.setDuration(60 + random.nextInt(240)); // 60-300 seconds
            event.setAgentId("agent-" + random.nextInt(100));
            event.setNpsScore(random.nextInt(2)); // 0 or 1
            sequence.add(event);
        }
        return sequence;
    }
}
