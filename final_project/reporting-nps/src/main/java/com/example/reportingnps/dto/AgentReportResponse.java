package com.example.reportingnps.dto;

import java.util.List;
import java.util.Map;

public class AgentReportResponse {
    private String agentId;
    private long totalCalls;
    private double averageNps;
    private Map<String, Long> callsByStatus;
    private Map<String, Long> callsBySegment;
    private long fraudAlerts;
    private boolean found;

    public AgentReportResponse() {}

    public AgentReportResponse(String agentId, long totalCalls, double averageNps,
                               Map<String, Long> callsByStatus, Map<String, Long> callsBySegment,
                               long fraudAlerts, boolean found) {
        this.agentId = agentId;
        this.totalCalls = totalCalls;
        this.averageNps = averageNps;
        this.callsByStatus = callsByStatus;
        this.callsBySegment = callsBySegment;
        this.fraudAlerts = fraudAlerts;
        this.found = found;
    }

    public String getAgentId() { return agentId; }
    public void setAgentId(String agentId) { this.agentId = agentId; }

    public long getTotalCalls() { return totalCalls; }
    public void setTotalCalls(long totalCalls) { this.totalCalls = totalCalls; }

    public double getAverageNps() { return averageNps; }
    public void setAverageNps(double averageNps) { this.averageNps = averageNps; }

    public Map<String, Long> getCallsByStatus() { return callsByStatus; }
    public void setCallsByStatus(Map<String, Long> callsByStatus) { this.callsByStatus = callsByStatus; }

    public Map<String, Long> getCallsBySegment() { return callsBySegment; }
    public void setCallsBySegment(Map<String, Long> callsBySegment) { this.callsBySegment = callsBySegment; }

    public long getFraudAlerts() { return fraudAlerts; }
    public void setFraudAlerts(long fraudAlerts) { this.fraudAlerts = fraudAlerts; }

    public boolean isFound() { return found; }
    public void setFound(boolean found) { this.found = found; }
}
