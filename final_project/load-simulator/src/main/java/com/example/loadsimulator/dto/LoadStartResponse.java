package com.example.loadsimulator.dto;

public class LoadStartResponse {

    private boolean started;
    private Configuration configuration;

    public LoadStartResponse(boolean started, Configuration configuration) {
        this.started = started;
        this.configuration = configuration;
    }

    public boolean isStarted() {
        return started;
    }

    public void setStarted(boolean started) {
        this.started = started;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public void setConfiguration(Configuration configuration) {
        this.configuration = configuration;
    }

    public static class Configuration {
        private int totalCalls;
        private int durationMinutes;
        private int burstSize;
        private int fraudPercent;

        public Configuration(int totalCalls, int durationMinutes, int burstSize, int fraudPercent) {
            this.totalCalls = totalCalls;
            this.durationMinutes = durationMinutes;
            this.burstSize = burstSize;
            this.fraudPercent = fraudPercent;
        }

        public int getTotalCalls() { return totalCalls; }
        public void setTotalCalls(int totalCalls) { this.totalCalls = totalCalls; }
        public int getDurationMinutes() { return durationMinutes; }
        public void setDurationMinutes(int durationMinutes) { this.durationMinutes = durationMinutes; }
        public int getBurstSize() { return burstSize; }
        public void setBurstSize(int burstSize) { this.burstSize = burstSize; }
        public int getFraudPercent() { return fraudPercent; }
        public void setFraudPercent(int fraudPercent) { this.fraudPercent = fraudPercent; }
    }
}
