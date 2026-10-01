package com.example.loadsimulator.service;

import com.example.loadsimulator.dto.CallEventRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manages a single LoadScheduler instance. Replaces the previous scheduler on each start.
 */
@Component
public class LoadSchedulerManager {

    private static final Logger log = LoggerFactory.getLogger(LoadSchedulerManager.class);
    private static final int DEFAULT_THREAD_POOL_SIZE = 10;

    private volatile LoadScheduler currentScheduler;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicInteger completedCalls = new AtomicInteger(0);
    private final AtomicInteger failedCalls = new AtomicInteger(0);

    private final @Value("${call-processor.url:http://localhost:8081}") String callProcessorUrl;

    public LoadSchedulerManager() {
        // Default constructor for Spring
        this.callProcessorUrl = "http://localhost:8081";
    }

    /**
     * Starts a new load generation with the given parameters.
     * Stops any existing generation first.
     */
    public void start(int totalCalls, int durationMinutes, int burstSize, int fraudPercent) {
        // Stop existing if running
        stop();

        running.set(true);
        completedCalls.set(0);
        failedCalls.set(0);

        PhoneGenerator phoneGenerator = new PhoneGenerator();
        FraudScenarioClassifier classifier = new FraudScenarioClassifier(fraudPercent);
        CallGenerator callGenerator = new CallGenerator(phoneGenerator);
        FrequentCallsCallFactory frequentFactory = new FrequentCallsCallFactory(phoneGenerator);
        NpsEscalationCallFactory npsFactory = new NpsEscalationCallFactory(phoneGenerator);
        AnomalousDurationCallFactory anomalousFactory = new AnomalousDurationCallFactory(phoneGenerator);
        CallProcessorClient client = new CallProcessorClient(callProcessorUrl, new com.fasterxml.jackson.databind.ObjectMapper());

        LoadScheduler scheduler = new LoadScheduler(
                totalCalls,
                durationMinutes,
                Math.min(burstSize, totalCalls),
                DEFAULT_THREAD_POOL_SIZE,
                callGenerator,
                frequentFactory,
                npsFactory,
                anomalousFactory,
                client,
                classifier
        );

        currentScheduler = scheduler;
        scheduler.start();
        log.info("Load generation started: {} calls over {} minutes, burstSize={}, fraudPercent={}",
                totalCalls, durationMinutes, burstSize, fraudPercent);
    }

    /**
     * Stops the current load generation.
     */
    public StopResult stop() {
        if (currentScheduler != null) {
            LoadScheduler.StopResult result = currentScheduler.stop();
            running.set(false);
            return new StopResult(result.completedCalls(), result.remainingCalls());
        }
        return new StopResult(0, 0);
    }

    /**
     * Returns current status.
     */
    public Status getStatus() {
        if (currentScheduler != null) {
            LoadScheduler.Status s = currentScheduler.getStatus();
            return new Status(s.running(), s.totalCalls(), completedCalls.get(), failedCalls.get(), s.percentage());
        }
        return new Status(false, 0, 0, 0, 0.0);
    }

    public boolean isRunning() {
        return running.get();
    }

    public record StopResult(int completedCalls, int remainingCalls) {}
    public record Status(boolean running, int totalCalls, int completedCalls, int failedCalls, double percentage) {}
}
