package com.example.loadsimulator.service;

import com.example.loadsimulator.dto.CallEventRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class NpsEscalationCallFactoryTest {

    private PhoneGenerator phoneGenerator;
    private NpsEscalationCallFactory factory;

    @BeforeEach
    void setUp() {
        phoneGenerator = new PhoneGenerator();
        factory = new NpsEscalationCallFactory(phoneGenerator);
    }

    @Test
    void generateSequence_hasAtLeastThreeCalls() {
        List<CallEventRequest> sequence = factory.generateSequence(0);

        assertTrue(sequence.size() >= 3, "Sequence must have at least 3 calls, got " + sequence.size());
    }

    @Test
    void generateSequence_allCallsHaveSamePhone() {
        List<CallEventRequest> sequence = factory.generateSequence(0);

        String firstPhone = sequence.get(0).getPhone();
        assertTrue(sequence.stream().allMatch(e -> e.getPhone().equals(firstPhone)),
                "All calls in sequence must have the same phone");
    }

    @Test
    void generateSequence_allCallsHaveLowNps() {
        List<CallEventRequest> sequence = factory.generateSequence(0);

        assertTrue(sequence.stream().allMatch(e -> e.getNpsScore() >= 0 && e.getNpsScore() < 2),
                "All calls must have npsScore 0 or 1");
    }
}
