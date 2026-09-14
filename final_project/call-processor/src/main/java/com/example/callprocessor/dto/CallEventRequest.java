package com.example.callprocessor.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;

public class CallEventRequest {

    @NotBlank(message = "callId is required")
    private String callId;

    @NotBlank(message = "phone is required")
    @Pattern(regexp = "^\\+?[1-9]\\d{1,14}$", message = "phone must be in E.164 format")
    private String phone;

    @Min(value = 1, message = "duration must be greater than 0")
    private Integer duration;

    @NotBlank(message = "agentId is required")
    private String agentId;

    @Min(value = 0, message = "npsScore must be between 0 and 10")
    @Max(value = 10, message = "npsScore must be between 0 and 10")
    private Integer npsScore;

    public CallEventRequest() {
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
}
