package com.example.callprocessor.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class CallEvent {

    private String callId;
    private String phone;
    private Integer duration;
    private String agentId;
    private Integer npsScore;
    private String status;
    private String timestamp;

    public CallEvent() {
    }

    public CallEvent(String callId, String phone, Integer duration, String agentId, Integer npsScore) {
        this.callId = callId;
        this.phone = phone;
        this.duration = duration;
        this.agentId = agentId;
        this.npsScore = npsScore;
        this.timestamp = java.time.Instant.now().toString();
    }

    public String getCallId() {
        return callId;
    }

    public void setCallId(String callId) {
        this.callId = callId;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public Integer getDuration() {
        return duration;
    }

    public void setDuration(Integer duration) {
        this.duration = duration;
    }

    public String getAgentId() {
        return agentId;
    }

    public void setAgentId(String agentId) {
        this.agentId = agentId;
    }

    public Integer getNpsScore() {
        return npsScore;
    }

    public void setNpsScore(Integer npsScore) {
        this.npsScore = npsScore;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }
}
