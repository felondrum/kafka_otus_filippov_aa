package com.example.loadsimulator.dto;

public class LoadStopResponse {

    private boolean stopped;
    private int completedCalls;
    private int remainingCalls;

    public LoadStopResponse(boolean stopped, int completedCalls, int remainingCalls) {
        this.stopped = stopped;
        this.completedCalls = completedCalls;
        this.remainingCalls = remainingCalls;
    }

    public boolean isStopped() {
        return stopped;
    }

    public void setStopped(boolean stopped) {
        this.stopped = stopped;
    }

    public int getCompletedCalls() {
        return completedCalls;
    }

    public void setCompletedCalls(int completedCalls) {
        this.completedCalls = completedCalls;
    }

    public int getRemainingCalls() {
        return remainingCalls;
    }

    public void setRemainingCalls(int remainingCalls) {
        this.remainingCalls = remainingCalls;
    }
}
