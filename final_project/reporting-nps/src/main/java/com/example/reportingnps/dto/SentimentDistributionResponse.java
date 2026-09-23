package com.example.reportingnps.dto;

import java.util.List;

public class SentimentDistributionResponse {
    private long positive;
    private long neutral;
    private long negative;
    private double positivePercent;
    private double neutralPercent;
    private double negativePercent;

    public SentimentDistributionResponse() {}

    public SentimentDistributionResponse(long positive, long neutral, long negative,
                                         double positivePercent, double neutralPercent, double negativePercent) {
        this.positive = positive;
        this.neutral = neutral;
        this.negative = negative;
        this.positivePercent = positivePercent;
        this.neutralPercent = neutralPercent;
        this.negativePercent = negativePercent;
    }

    public long getPositive() { return positive; }
    public void setPositive(long positive) { this.positive = positive; }

    public long getNeutral() { return neutral; }
    public void setNeutral(long neutral) { this.neutral = neutral; }

    public long getNegative() { return negative; }
    public void setNegative(long negative) { this.negative = negative; }

    public double getPositivePercent() { return positivePercent; }
    public void setPositivePercent(double positivePercent) { this.positivePercent = positivePercent; }

    public double getNeutralPercent() { return neutralPercent; }
    public void setNeutralPercent(double neutralPercent) { this.neutralPercent = neutralPercent; }

    public double getNegativePercent() { return negativePercent; }
    public void setNegativePercent(double negativePercent) { this.negativePercent = negativePercent; }
}
