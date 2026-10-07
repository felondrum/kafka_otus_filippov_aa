package com.example.reportingnps.dto;

import java.util.List;
import java.util.Map;

public class DailyReportResponse {
    private long totalCalls;
    private double averageNps;
    private Map<String, Long> callsByStatus;
    private Map<String, Long> callsBySegment;
    private Map<String, Long> callsByAgent;

    public DailyReportResponse() {}

    public DailyReportResponse(long totalCalls, double averageNps, Map<String, Long> callsByStatus,
                               Map<String, Long> callsBySegment, Map<String, Long> callsByAgent) {
        this.totalCalls = totalCalls;
        this.averageNps = averageNps;
        this.callsByStatus = callsByStatus;
        this.callsBySegment = callsBySegment;
        this.callsByAgent = callsByAgent;
    }

    public long getTotalCalls() { return totalCalls; }
    public void setTotalCalls(long totalCalls) { this.totalCalls = totalCalls; }

    public double getAverageNps() { return averageNps; }
    public void setAverageNps(double averageNps) { this.averageNps = averageNps; }

    public Map<String, Long> getCallsByStatus() { return callsByStatus; }
    public void setCallsByStatus(Map<String, Long> callsByStatus) { this.callsByStatus = callsByStatus; }

    public Map<String, Long> getCallsBySegment() { return callsBySegment; }
    public void setCallsBySegment(Map<String, Long> callsBySegment) { this.callsBySegment = callsBySegment; }

    public Map<String, Long> getCallsByAgent() { return callsByAgent; }
    public void setCallsByAgent(Map<String, Long> callsByAgent) { this.callsByAgent = callsByAgent; }
}
