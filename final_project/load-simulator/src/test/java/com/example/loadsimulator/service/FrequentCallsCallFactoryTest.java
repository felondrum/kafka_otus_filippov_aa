package com.example.loadsimulator.service;

import com.example.loadsimulator.dto.CallEventRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class FrequentCallsCallFactoryTest {

    private PhoneGenerator phoneGenerator;
    private FrequentCallsCallFactory factory;

    @BeforeEach
    void setUp() {
        phoneGenerator = new PhoneGenerator();
        factory = new FrequentCallsCallFactory(phoneGenerator);
    }

    @Test
    void generateBurst_hasAtLeastSixCalls() {
        List<CallEventRequest> burst = factory.generateBurst(0);

        assertTrue(burst.size() >= 6, "Burst must have at least 6 calls, got " + burst.size());
    }

    @Test
    void generateBurst_allCallsHaveSamePhone() {
        List<CallEventRequest> burst = factory.generateBurst(0);

        String firstPhone = burst.get(0).getPhone();
        assertTrue(burst.stream().allMatch(e -> e.getPhone().equals(firstPhone)),
                "All calls in burst must have the same phone");
    }

    @Test
    void generateBurst_allCallsHaveValidFields() {
        List<CallEventRequest> burst = factory.generateBurst(0);

        assertTrue(burst.stream().allMatch(e ->
                e.getCallId() != null && e.getPhone() != null
                        && e.getDuration() != null && e.getAgentId() != null));
    }
}
