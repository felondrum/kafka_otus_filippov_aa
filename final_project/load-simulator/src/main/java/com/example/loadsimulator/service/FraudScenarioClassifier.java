package com.example.loadsimulator.service;



import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Classifies each call into a fraud scenario based on the configured fraudPercent.
 * Distribution: NORMAL = (100 - fraudPercent)%,
 * ANOMALOUS_DURATION = fraudPercent / 3%,
 * FREQUENT_CALLS = fraudPercent / 3%,
 * NPS_ESCALATION = fraudPercent / 3%.
 */
public class FraudScenarioClassifier {

    private final int fraudPercent;

    public FraudScenarioClassifier(int fraudPercent) {
        this.fraudPercent = fraudPercent;
    }

    public FraudScenario classify() {
        int value = ThreadLocalRandom.current().nextInt(100);
        if (value < fraudPercent) {
            // Within fraud range, distribute evenly among 3 scenarios
            int scenario = ThreadLocalRandom.current().nextInt(3);
            return switch (scenario) {
                case 0 -> FraudScenario.ANOMALOUS_DURATION;
                case 1 -> FraudScenario.FREQUENT_CALLS;
                case 2 -> FraudScenario.NPS_ESCALATION;
                default -> FraudScenario.NORMAL;
            };
        }
        return FraudScenario.NORMAL;
    }

    public int getFraudPercent() {
        return fraudPercent;
    }
}
