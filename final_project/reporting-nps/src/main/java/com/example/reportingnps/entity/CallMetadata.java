package com.example.reportingnps.entity;

import jakarta.persistence.*;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = "call_metadata")
public class CallMetadata {

    @Id
    @Column(name = "call_id")
    private UUID callId;

    @Column(name = "customer_phone")
    private String customerPhone;

    @Column(name = "agent_id")
    private String agentId;

    @Column(name = "call_start_time")
    private LocalDateTime callStartTime;

    @Column(name = "call_end_time")
    private LocalDateTime callEndTime;

    @Column(name = "call_duration")
    private Integer callDuration;

    @Column(name = "call_status")
    private String callStatus;

    @Column(name = "queue_name")
    private String queueName;

    @Column(name = "ivr_selection")
    private String ivrSelection;

    @Column(name = "call_purpose")
    private String callPurpose;

    @Column(name = "sentiment_score")
    private BigDecimal sentimentScore;

    @Column(name = "sentiment")
    private String sentiment;

    @Column(name = "urgency")
    private String urgency;

    @Column(name = "problem")
    private String problem;

    @Column(name = "solution")
    private String solution;

    @Column(name = "confidence")
    private Double confidence;

    @Column(name = "segment")
    private String segment;

    @Column(name = "risk_level")
    private String riskLevel;

    @Column(name = "priority")
    private String priority;

    @Column(name = "created_at")
    private LocalDateTime createdAt;

    @Column(name = "updated_at")
    private LocalDateTime updatedAt;

    @PrePersist
    protected void onCreate() {
        createdAt = LocalDateTime.now();
        updatedAt = LocalDateTime.now();
    }

    @PreUpdate
    protected void onUpdate() {
        updatedAt = LocalDateTime.now();
    }

    // Getters and setters
    public UUID getCallId() { return callId; }
    public void setCallId(UUID callId) { this.callId = callId; }

    public String getCustomerPhone() { return customerPhone; }
    public void setCustomerPhone(String customerPhone) { this.customerPhone = customerPhone; }

    public String getAgentId() { return agentId; }
    public void setAgentId(String agentId) { this.agentId = agentId; }

    public LocalDateTime getCallStartTime() { return callStartTime; }
    public void setCallStartTime(LocalDateTime callStartTime) { this.callStartTime = callStartTime; }

    public LocalDateTime getCallEndTime() { return callEndTime; }
    public void setCallEndTime(LocalDateTime callEndTime) { this.callEndTime = callEndTime; }

    public Integer getCallDuration() { return callDuration; }
    public void setCallDuration(Integer callDuration) { this.callDuration = callDuration; }

    public String getCallStatus() { return callStatus; }
    public void setCallStatus(String callStatus) { this.callStatus = callStatus; }

    public String getQueueName() { return queueName; }
    public void setQueueName(String queueName) { this.queueName = queueName; }

    public String getIvrSelection() { return ivrSelection; }
    public void setIvrSelection(String ivrSelection) { this.ivrSelection = ivrSelection; }

    public String getCallPurpose() { return callPurpose; }
    public void setCallPurpose(String callPurpose) { this.callPurpose = callPurpose; }

    public BigDecimal getSentimentScore() { return sentimentScore; }
    public void setSentimentScore(BigDecimal sentimentScore) { this.sentimentScore = sentimentScore; }

    public String getSentiment() { return sentiment; }
    public void setSentiment(String sentiment) { this.sentiment = sentiment; }

    public String getUrgency() { return urgency; }
    public void setUrgency(String urgency) { this.urgency = urgency; }

    public String getProblem() { return problem; }
    public void setProblem(String problem) { this.problem = problem; }

    public String getSolution() { return solution; }
    public void setSolution(String solution) { this.solution = solution; }

    public Double getConfidence() { return confidence; }
    public void setConfidence(Double confidence) { this.confidence = confidence; }

    public String getSegment() { return segment; }
    public void setSegment(String segment) { this.segment = segment; }

    public String getRiskLevel() { return riskLevel; }
    public void setRiskLevel(String riskLevel) { this.riskLevel = riskLevel; }

    public String getPriority() { return priority; }
    public void setPriority(String priority) { this.priority = priority; }

    public LocalDateTime getCreatedAt() { return createdAt; }
    public void setCreatedAt(LocalDateTime createdAt) { this.createdAt = createdAt; }

    public LocalDateTime getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(LocalDateTime updatedAt) { this.updatedAt = updatedAt; }
}
