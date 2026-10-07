package com.example.transcription.analyzer.dto;

public class CallEventRequest {
    private String callId;
    private String phone;
    private double duration;
    private String agentId;
    private int npsScore;

    public CallEventRequest() {}

    public String getCallId() { return callId; }
    public void setCallId(String callId) { this.callId = callId; }
    public String getPhone() { return phone; }
    public void setPhone(String phone) { this.phone = phone; }
    public double getDuration() { return duration; }
    public void setDuration(double duration) { this.duration = duration; }
    public String getAgentId() { return agentId; }
    public void setAgentId(String agentId) { this.agentId = agentId; }
    public int getNpsScore() { return npsScore; }
    public void setNpsScore(int npsScore) { this.npsScore = npsScore; }
}
