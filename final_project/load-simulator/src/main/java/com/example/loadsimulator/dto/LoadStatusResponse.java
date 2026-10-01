package com.example.loadsimulator.dto;

public class LoadStatusResponse {

    private boolean running;
    private int totalCalls;
    private int completedCalls;
    private int failedCalls;
    private double percentage;

    public LoadStatusResponse(boolean running, int totalCalls, int completedCalls, int failedCalls, double percentage) {
        this.running = running;
        this.totalCalls = totalCalls;
        this.completedCalls = completedCalls;
        this.failedCalls = failedCalls;
        this.percentage = percentage;
    }

    public boolean isRunning() {
        return running;
    }

    public void setRunning(boolean running) {
        this.running = running;
    }

    public int getTotalCalls() {
        return totalCalls;
    }

    public void setTotalCalls(int totalCalls) {
        this.totalCalls = totalCalls;
    }

    public int getCompletedCalls() {
        return completedCalls;
    }

    public void setCompletedCalls(int completedCalls) {
        this.completedCalls = completedCalls;
    }

    public int getFailedCalls() {
        return failedCalls;
    }

    public void setFailedCalls(int failedCalls) {
        this.failedCalls = failedCalls;
    }

    public double getPercentage() {
        return percentage;
    }

    public void setPercentage(double percentage) {
        this.percentage = percentage;
    }
}
