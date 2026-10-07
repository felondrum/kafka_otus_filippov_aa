package com.example.loadsimulator.service;

import com.example.loadsimulator.dto.CallEventRequest;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Generates a pool of diverse Russian phone numbers in E.164 format.
 */
public class PhoneGenerator {

    private static final List<String> PHONE_POOL = List.of(
            "+79001000001", "+79001000002", "+79001000003", "+79001000004", "+79001000005",
            "+79011000001", "+79011000002", "+79011000003", "+79011000004", "+79011000005",
            "+79021000001", "+79021000002", "+79021000003", "+79021000004", "+79021000005",
            "+79031000001", "+79031000002", "+79031000003", "+79031000004", "+79031000005",
            "+79041000001", "+79041000002", "+79041000003", "+79041000004", "+79041000005",
            "+79051000001", "+79051000002", "+79051000003", "+79051000004", "+79051000005",
            "+79061000001", "+79061000002", "+79061000003", "+79061000004", "+79061000005",
            "+79071000001", "+79071000002", "+79071000003", "+79071000004", "+79071000005",
            "+79081000001", "+79081000002", "+79081000003", "+79081000004", "+79081000005",
            "+79091000001", "+79091000002", "+79091000003", "+79091000004", "+79091000005"
    );

    private final ThreadLocalRandom random = ThreadLocalRandom.current();

    public String generateRandomPhone() {
        return PHONE_POOL.get(random.nextInt(PHONE_POOL.size()));
    }

    public String generateFrequentCallPhone() {
        // Use first 3 numbers for frequent call scenarios
        return PHONE_POOL.get(random.nextInt(3));
    }

    public String generateNpsEscalationPhone() {
        // Use next 2 numbers for NPS escalation scenarios
        return PHONE_POOL.get(3 + random.nextInt(2));
    }

    public int size() {
        return PHONE_POOL.size();
    }
}
