package com.example.loadsimulator.service;

import com.example.loadsimulator.dto.CallEventRequest;
import com.example.loadsimulator.service.CallProcessorClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Orchestrates burst-based call generation with jitter-based intervals.
 * Manages thread pool, fraud scenario distribution, and fire-and-forget HTTP calls.
 */
public class LoadScheduler {

    private static final Logger log = LoggerFactory.getLogger(LoadScheduler.class);
    private static final double JITTER_PERCENT = 0.30;

    private final int totalCalls;
    private final int durationMinutes;
    private final int burstSize;
    private final int threadPoolSize;
    private final CallGenerator callGenerator;
    private final FrequentCallsCallFactory frequentCallsFactory;
    private final NpsEscalationCallFactory npsEscalationFactory;
    private final AnomalousDurationCallFactory anomalousDurationFactory;
    private final CallProcessorClient callProcessorClient;
    private final FraudScenarioClassifier classifier;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicInteger completedCalls = new AtomicInteger(0);
    private final AtomicInteger failedCalls = new AtomicInteger(0);
    private final AtomicInteger generatedCalls = new AtomicInteger(0);
    private ExecutorService executorService;
    private volatile Thread schedulerThread;

    public LoadScheduler(int totalCalls, int durationMinutes, int burstSize, int threadPoolSize,
                         CallGenerator callGenerator, FrequentCallsCallFactory frequentCallsFactory,
                         NpsEscalationCallFactory npsEscalationFactory,
                         AnomalousDurationCallFactory anomalousDurationFactory,
                         CallProcessorClient callProcessorClient,
                         FraudScenarioClassifier classifier) {
        this.totalCalls = totalCalls;
        this.durationMinutes = durationMinutes;
        this.burstSize = Math.min(burstSize, totalCalls);
        this.threadPoolSize = threadPoolSize;
        this.callGenerator = callGenerator;
        this.frequentCallsFactory = frequentCallsFactory;
        this.npsEscalationFactory = npsEscalationFactory;
        this.anomalousDurationFactory = anomalousDurationFactory;
        this.callProcessorClient = callProcessorClient;
        this.classifier = classifier;
    }

    /**
     * Starts the load generation in a background thread.
     */
    public void start() {
        if (!running.compareAndSet(false, true)) {
            throw new IllegalStateException("Load generation is already running");
        }

        executorService = Executors.newFixedThreadPool(threadPoolSize, r -> {
            Thread t = new Thread(r, "load-simulator-pool");
            t.setDaemon(true);
            return t;
        });

        schedulerThread = new Thread(this::runGeneration, "load-scheduler");
        schedulerThread.start();
        log.info("Load generation started: {} calls over {} minutes, burstSize={}, fraudPercent={}",
                totalCalls, durationMinutes, burstSize, classifier.getFraudPercent());
    }

    /**
     * Stops the load generation immediately.
     */
    public StopResult stop() {
        running.set(false);
        if (schedulerThread != null && schedulerThread.isAlive()) {
            schedulerThread.interrupt();
        }
        if (executorService != null) {
            executorService.shutdownNow();
            try {
                if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                    executorService.shutdown();
                }
            } catch (InterruptedException e) {
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        int completed = completedCalls.get();
        int remaining = Math.max(0, totalCalls - completed);
        log.info("Load generation stopped: completed={}, remaining={}", completed, remaining);
        return new StopResult(completed, remaining);
    }

    /**
     * Returns current status.
     */
    public Status getStatus() {
        int completed = completedCalls.get();
        int failed = failedCalls.get();
        double percentage = totalCalls > 0 ? (completed * 100.0 / totalCalls) : 0.0;
        return new Status(running.get(), totalCalls, completed, failed, percentage);
    }

    private void runGeneration() {
        try {
            int numBursts = (int) Math.ceil((double) totalCalls / burstSize);
            long baseIntervalMs = durationMinutes * 60_000L / numBursts;

            // Pre-calculate fraud call counts
            int fraudCallCount = (int) Math.round(totalCalls * classifier.getFraudPercent() / 100.0);
            int normalCallCount = totalCalls - fraudCallCount;

            // Fraud calls are split evenly among 3 scenarios
            int anomalousDurationCalls = fraudCallCount / 3;
            int frequentCallsBursts = (fraudCallCount / 3) / 6; // each burst = 6+ calls
            int npsEscalationSequences = (fraudCallCount / 3) / 3; // each sequence = 3+ calls

            // Generate fraud call events upfront
            List<CallEventRequest> anomalousDurationCallsList = new ArrayList<>();
            for (int i = 0; i < anomalousDurationCalls; i++) {
                anomalousDurationCallsList.add(anomalousDurationFactory.generate(i));
            }

            List<CallEventRequest> frequentCallsList = new ArrayList<>();
            for (int i = 0; i < frequentCallsBursts; i++) {
                frequentCallsList.addAll(frequentCallsFactory.generateBurst(i));
            }

            List<CallEventRequest> npsEscalationList = new ArrayList<>();
            for (int i = 0; i < npsEscalationSequences; i++) {
                npsEscalationList.addAll(npsEscalationFactory.generateSequence(i));
            }

            // Combine all fraud calls
            List<CallEventRequest> fraudCalls = new ArrayList<>();
            fraudCalls.addAll(anomalousDurationCallsList);
            fraudCalls.addAll(frequentCallsList);
            fraudCalls.addAll(npsEscalationList);

            // Generate normal calls
            List<CallEventRequest> normalCalls = callGenerator.generateBatch(normalCallCount);

            // Interleave fraud and normal calls
            List<CallEventRequest> allCalls = new ArrayList<>(totalCalls);
            int fraudIdx = 0, normalIdx = 0;
            int totalGenerated = fraudCalls.size() + normalCalls.size();
            
            // Add all fraud calls first
            while (fraudIdx < fraudCalls.size()) {
                allCalls.add(fraudCalls.get(fraudIdx++));
            }
            // Add all normal calls
            while (normalIdx < normalCalls.size()) {
                allCalls.add(normalCalls.get(normalIdx++));
            }
            
            // Shuffle to mix fraud and normal calls
            java.util.Collections.shuffle(allCalls);
            
            // Ensure we don't exceed totalCalls
            while (allCalls.size() > totalCalls) {
                allCalls.remove(allCalls.size() - 1);
            }

            // Send calls in bursts
            for (int burstIdx = 0; burstIdx < numBursts && running.get(); burstIdx++) {
                int from = burstIdx * burstSize;
                int to = Math.min(from + burstSize, totalCalls);
                List<CallEventRequest> burst = allCalls.subList(from, to);

                // Apply jitter to interval
                long jitteredInterval = applyJitter(baseIntervalMs);
                if (burstIdx > 0 && jitteredInterval > 0) {
                    Thread.sleep(jitteredInterval);
                }

                // Submit burst calls to thread pool
                List<Future<?>> futures = new ArrayList<>();
                for (CallEventRequest call : burst) {
                    futures.add(executorService.submit(() -> {
                        try {
                            callProcessorClient.sendCall(call);
                            completedCalls.incrementAndGet();
                        } catch (Exception e) {
                            failedCalls.incrementAndGet();
                            log.warn("Failed to send call {}: {}", call.getCallId(), e.getMessage());
                        }
                    }));
                }

                // Wait for burst to complete before next interval
                for (Future<?> f : futures) {
                    try {
                        f.get(15, TimeUnit.SECONDS);
                    } catch (TimeoutException e) {
                        f.cancel(true);
                        completedCalls.incrementAndGet(); // count timed-out as completed to not block
                    }
                }

                generatedCalls.set((burstIdx + 1) * burstSize);
            }

            log.info("Load generation completed: {} calls sent", totalCalls);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.info("Load generation interrupted");
        } catch (Exception e) {
            log.error("Load generation failed", e);
        } finally {
            running.set(false);
        }
    }

    private long applyJitter(long baseIntervalMs) {
        if (baseIntervalMs <= 0) return 0;
        double jitterRange = baseIntervalMs * JITTER_PERCENT;
        long lowerBound = (long) (baseIntervalMs - jitterRange);
        long upperBound = (long) (baseIntervalMs + jitterRange);
        return Math.max(0, lowerBound + (long) (Math.random() * (upperBound - lowerBound + 1)));
    }

    public record StopResult(int completedCalls, int remainingCalls) {}
    public record Status(boolean running, int totalCalls, int completedCalls, int failedCalls, double percentage) {}
}
