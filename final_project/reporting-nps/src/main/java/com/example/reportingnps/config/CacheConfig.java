package com.example.reportingnps.config;

import com.github.benmanes.caffeine.cache.Caffeine;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.CacheManager;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.cache.annotation.EnableCaching;

import java.util.Arrays;

@Configuration
@EnableCaching
public class CacheConfig {

    @Value("${app.cache.metadata-ttl-minutes:5}")
    private long metadataTtlMinutes;

    @Value("${app.cache.report-ttl-minutes:1}")
    private long reportTtlMinutes;

    @Value("${app.cache.sentiment-ttl-minutes:1}")
    private long sentimentTtlMinutes;

    @Bean
    public CacheManager cacheManager() {
        CaffeineCacheManager manager = new CaffeineCacheManager();
        manager.setCacheNames(Arrays.asList("metadataCache", "reportCache", "sentimentCache"));
        manager.setCaffeine(Caffeine.newBuilder()
                .maximumSize(1000)
        );

        // Configure specific cache TTLs via custom cache wrappers
        var metadataCache = Caffeine.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(java.time.Duration.ofMinutes(metadataTtlMinutes))
                .build();
        var reportCache = Caffeine.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(java.time.Duration.ofMinutes(reportTtlMinutes))
                .build();
        var sentimentCache = Caffeine.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(java.time.Duration.ofMinutes(sentimentTtlMinutes))
                .build();

        manager.setCacheNames(Arrays.asList("metadataCache", "reportCache", "sentimentCache"));
        manager.setCustomCaches(Arrays.asList(
                new com.github.benmanes.caffeine.cache.Cache<String, Object>() {},
                new com.github.benmanes.caffeine.cache.Cache<String, Object>() {},
                new com.github.benmanes.caffeine.cache.Cache<String, Object>() {}
        ));

        return manager;
    }
}
