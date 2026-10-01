package com.example.loadsimulator.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

class FraudScenarioClassifierTest {

    private FraudScenarioClassifier classifier;

    @BeforeEach
    void setUp() {
        classifier = new FraudScenarioClassifier(15);
    }

    @Test
    void classify_returnsOnlyValidScenarios() {
        List<FraudScenario> results = IntStream.range(0, 10000)
                .mapToObj(i -> classifier.classify())
                .collect(Collectors.toList());

        assertTrue(results.stream().allMatch(s ->
                s == FraudScenario.NORMAL
                        || s == FraudScenario.ANOMALOUS_DURATION
                        || s == FraudScenario.FREQUENT_CALLS
                        || s == FraudScenario.NPS_ESCALATION));
    }

    @Test
    void classify_distributionApproximatelyCorrect() {
        int iterations = 10000;
        List<FraudScenario> results = IntStream.range(0, iterations)
                .mapToObj(i -> classifier.classify())
                .collect(Collectors.toList());

        long normalCount = results.stream().filter(s -> s == FraudScenario.NORMAL).count();
        long fraudCount = results.stream().filter(s -> s != FraudScenario.NORMAL).count();

        double fraudPercent = (fraudCount * 100.0) / iterations;

        // Allow ±5% tolerance
        assertTrue(fraudPercent >= 10.0 && fraudPercent <= 20.0,
                "Fraud percent should be approximately 15%, got " + fraudPercent);
    }

    @Test
    void getFraudPercent_returnsConfiguredValue() {
        assertEquals(15, classifier.getFraudPercent());
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 15, 50, 100})
    void classifier_withDifferentFraudPercent(int fraudPercent) {
        FraudScenarioClassifier c = new FraudScenarioClassifier(fraudPercent);

        int iterations = 10000;
        long fraudCount = IntStream.range(0, iterations)
                .mapToObj(i -> c.classify())
                .filter(s -> s != FraudScenario.NORMAL)
                .count();

        double actualPercent = (fraudCount * 100.0) / iterations;

        // Allow ±10% tolerance for wider fraud ranges
        double tolerance = fraudPercent == 0 || fraudPercent == 100 ? 5 : 10;
        assertTrue(Math.abs(actualPercent - fraudPercent) <= tolerance,
                "Fraud percent should be approximately " + fraudPercent + "%, got " + actualPercent);
    }
}
