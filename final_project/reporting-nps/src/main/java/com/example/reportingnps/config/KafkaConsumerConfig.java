package com.example.reportingnps.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import java.util.Map;

@Configuration
public class KafkaConsumerConfig {

    @Value("${app.consumer.processing-timeout-seconds:30}")
    private int processingTimeoutSeconds;

    @Bean
    public RetryTemplate retryTemplate() {
        RetryTemplate template = new RetryTemplate();

        // Exponential backoff: 1s, 2s, 4s
        ExponentialBackOffPolicy backOff = new ExponentialBackOffPolicy();
        backOff.setInitialInterval(1000L);
        backOff.setMultiplier(2.0);
        backOff.setMaxInterval(4000L);
        template.setBackOffPolicy(backOff);

        // 3 attempts
        SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy(3, Map.of(Exception.class, 3));
        template.setRetryPolicy(retryPolicy);

        return template;
    }
}
