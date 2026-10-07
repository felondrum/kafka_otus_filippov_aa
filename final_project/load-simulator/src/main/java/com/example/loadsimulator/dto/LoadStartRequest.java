package com.example.loadsimulator.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

public class LoadStartRequest {

    @NotNull(message = "totalCalls is required")
    @Min(value = 1, message = "totalCalls must be greater than 0")
    private Integer totalCalls;

    @NotNull(message = "durationMinutes is required")
    @Min(value = 1, message = "durationMinutes must be greater than 0")
    private Integer durationMinutes;

    @Min(value = 1, message = "burstSize must be greater than 0")
    private Integer burstSize = 20;

    @Min(value = 0, message = "fraudPercent must be between 0 and 100")
    @Max(value = 100, message = "fraudPercent must be between 0 and 100")
    private Integer fraudPercent = 15;

    public LoadStartRequest() {
    }

    public Integer getTotalCalls() {
        return totalCalls;
    }

    public void setTotalCalls(Integer totalCalls) {
        this.totalCalls = totalCalls;
    }

    public Integer getDurationMinutes() {
        return durationMinutes;
    }

    public void setDurationMinutes(Integer durationMinutes) {
        this.durationMinutes = durationMinutes;
    }

    public Integer getBurstSize() {
        return burstSize;
    }

    public void setBurstSize(Integer burstSize) {
        this.burstSize = burstSize;
    }

    public Integer getFraudPercent() {
        return fraudPercent;
    }

    public void setFraudPercent(Integer fraudPercent) {
        this.fraudPercent = fraudPercent;
    }
}
