package com.example.reportingnps.entity;

import jakarta.persistence.*;
import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = "call_transcriptions")
public class CallTranscription {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(name = "transcription_id")
    private UUID transcriptionId;

    @Column(name = "call_id", nullable = false)
    private UUID callId;

    @Column(name = "transcription_text")
    private String transcriptionText;

    @Column(name = "language")
    private String language;

    @Column(name = "confidence_score")
    private Double confidenceScore;

    @Column(name = "problem")
    private String problem;

    @Column(name = "solution")
    private String solution;

    @Column(name = "sentiment")
    private String sentiment;

    @Column(name = "urgency")
    private String urgency;

    @Column(name = "confidence")
    private Double confidence;

    @Column(name = "segment")
    private String segment;

    @Column(name = "risk_level")
    private String riskLevel;

    @Column(name = "priority")
    private String priority;

    @Column(name = "processing_status")
    private String processingStatus;

    @Column(name = "error_message")
    private String errorMessage;

    @Column(name = "created_at")
    private LocalDateTime createdAt;

    @Column(name = "updated_at")
    private LocalDateTime updatedAt;

    @PrePersist
    protected void onCreate() {
        createdAt = LocalDateTime.now();
        updatedAt = LocalDateTime.now();
        if (processingStatus == null) {
            processingStatus = "PENDING";
        }
    }

    @PreUpdate
    protected void onUpdate() {
        updatedAt = LocalDateTime.now();
    }

    // Getters and setters
    public UUID getTranscriptionId() { return transcriptionId; }
    public void setTranscriptionId(UUID transcriptionId) { this.transcriptionId = transcriptionId; }

    public UUID getCallId() { return callId; }
    public void setCallId(UUID callId) { this.callId = callId; }

    public String getTranscriptionText() { return transcriptionText; }
    public void setTranscriptionText(String transcriptionText) { this.transcriptionText = transcriptionText; }

    public String getLanguage() { return language; }
    public void setLanguage(String language) { this.language = language; }

    public Double getConfidenceScore() { return confidenceScore; }
    public void setConfidenceScore(Double confidenceScore) { this.confidenceScore = confidenceScore; }

    public String getProblem() { return problem; }
    public void setProblem(String problem) { this.problem = problem; }

    public String getSolution() { return solution; }
    public void setSolution(String solution) { this.solution = solution; }

    public String getSentiment() { return sentiment; }
    public void setSentiment(String sentiment) { this.sentiment = sentiment; }

    public String getUrgency() { return urgency; }
    public void setUrgency(String urgency) { this.urgency = urgency; }

    public Double getConfidence() { return confidence; }
    public void setConfidence(Double confidence) { this.confidence = confidence; }

    public String getSegment() { return segment; }
    public void setSegment(String segment) { this.segment = segment; }

    public String getRiskLevel() { return riskLevel; }
    public void setRiskLevel(String riskLevel) { this.riskLevel = riskLevel; }

    public String getPriority() { return priority; }
    public void setPriority(String priority) { this.priority = priority; }

    public String getProcessingStatus() { return processingStatus; }
    public void setProcessingStatus(String processingStatus) { this.processingStatus = processingStatus; }

    public String getErrorMessage() { return errorMessage; }
    public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }

    public LocalDateTime getCreatedAt() { return createdAt; }
    public void setCreatedAt(LocalDateTime createdAt) { this.createdAt = createdAt; }

    public LocalDateTime getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(LocalDateTime updatedAt) { this.updatedAt = updatedAt; }
}
