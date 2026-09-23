package com.example.reportingnps.entity;

import jakarta.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "fraud_stats")
public class FraudStats {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "phone", nullable = false)
    private String phone;

    @Column(name = "call_id")
    private String callId;

    @Column(name = "pattern", nullable = false)
    private String pattern;

    @Column(name = "severity", nullable = false)
    private String severity;

    @Column(name = "count", nullable = false)
    private Integer count;

    @Column(name = "last_alert_at")
    private LocalDateTime lastAlertAt;

    @Column(name = "agent_id")
    private String agentId;

    @PrePersist
    protected void onCreate() {
        lastAlertAt = LocalDateTime.now();
    }

    @PreUpdate
    protected void onUpdate() {
        lastAlertAt = LocalDateTime.now();
    }

    // Getters and setters
    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }

    public String getPhone() { return phone; }
    public void setPhone(String phone) { this.phone = phone; }

    public String getCallId() { return callId; }
    public void setCallId(String callId) { this.callId = callId; }

    public String getPattern() { return pattern; }
    public void setPattern(String pattern) { this.pattern = pattern; }

    public String getSeverity() { return severity; }
    public void setSeverity(String severity) { this.severity = severity; }

    public Integer getCount() { return count; }
    public void setCount(Integer count) { this.count = count; }

    public LocalDateTime getLastAlertAt() { return lastAlertAt; }
    public void setLastAlertAt(LocalDateTime lastAlertAt) { this.lastAlertAt = lastAlertAt; }

    public String getAgentId() { return agentId; }
    public void setAgentId(String agentId) { this.agentId = agentId; }
}
