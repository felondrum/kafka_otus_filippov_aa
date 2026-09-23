package com.example.frauddetector.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
public class FraudDetectorProperties {

    @Value("${fraud-detector.topics.completed:calls.completed}")
    private String completedTopic;

    @Value("${fraud-detector.topics.fraud-alerts:calls.fraud-alerts}")
    private String fraudAlertsTopic;

    @Value("${fraud-detector.windows.frequent-calls.size-ms:60000}")
    private long frequentCallsWindowSizeMs;

    @Value("${fraud-detector.windows.frequent-calls.advance-ms:10000}")
    private long frequentCallsAdvanceMs;

    @Value("${fraud-detector.windows.frequent-calls.threshold:5}")
    private int frequentCallsThreshold;

    @Value("${fraud-detector.windows.nps-escalation.window-hours:24}")
    private int npsEscalationWindowHours;

    @Value("${fraud-detector.windows.nps-escalation.threshold:3}")
    private int npsEscalationThreshold;

    @Value("${fraud-detector.filters.min-duration-seconds:5}")
    private int minDurationSeconds;

    @Value("${fraud-detector.filters.max-duration-seconds:300}")
    private int maxDurationSeconds;

    public String getCompletedTopic() {
        return completedTopic;
    }

    public String getFraudAlertsTopic() {
        return fraudAlertsTopic;
    }

    public long getFrequentCallsWindowSizeMs() {
        return frequentCallsWindowSizeMs;
    }

    public long getFrequentCallsAdvanceMs() {
        return frequentCallsAdvanceMs;
    }

    public int getFrequentCallsThreshold() {
        return frequentCallsThreshold;
    }

    public int getNpsEscalationWindowHours() {
        return npsEscalationWindowHours;
    }

    public int getNpsEscalationThreshold() {
        return npsEscalationThreshold;
    }

    public int getMinDurationSeconds() {
        return minDurationSeconds;
    }

    public int getMaxDurationSeconds() {
        return maxDurationSeconds;
    }
}
