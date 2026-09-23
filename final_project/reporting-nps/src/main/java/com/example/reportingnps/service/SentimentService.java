package com.example.reportingnps.service;

import com.example.reportingnps.dto.SentimentDistributionResponse;
import com.example.reportingnps.repository.CallTranscriptionRepository;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Service
public class SentimentService {

    private final CallTranscriptionRepository callTranscriptionRepository;

    public SentimentService(CallTranscriptionRepository callTranscriptionRepository) {
        this.callTranscriptionRepository = callTranscriptionRepository;
    }

    @Cacheable(value = "sentimentCache", key = "'sentiment:' + #from + ':' + #to")
    public SentimentDistributionResponse getSentimentDistribution(LocalDateTime from, LocalDateTime to) {
        long positive = callTranscriptionRepository.countPositiveBetween(from, to);
        long neutral = callTranscriptionRepository.countNeutralBetween(from, to);
        long negative = callTranscriptionRepository.countNegativeBetween(from, to);
        long total = positive + neutral + negative;

        double positivePercent = total > 0 ? (positive * 100.0 / total) : 0.0;
        double neutralPercent = total > 0 ? (neutral * 100.0 / total) : 0.0;
        double negativePercent = total > 0 ? (negative * 100.0 / total) : 0.0;

        return new SentimentDistributionResponse(positive, neutral, negative,
                positivePercent, neutralPercent, negativePercent);
    }

    @CacheEvict(value = "sentimentCache", allEntries = true)
    public void evictSentimentCache() {
        // All sentiment cache entries are evicted
    }
}
