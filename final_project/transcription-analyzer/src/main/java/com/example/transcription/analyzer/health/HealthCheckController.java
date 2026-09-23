package com.example.transcription.analyzer.health;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.health.layer.Layer;
import org.springframework.health.LayerStatus;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@RestController
@RequestMapping("/api")
public class HealthCheckController {

    @Autowired(required = false)
    private KafkaTemplate<String, ?> kafkaTemplate;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @GetMapping("/health")
    public Map<String, Object> health() {
        Map<String, Object> status = new HashMap<>();
        status.put("status", "UP");

        // Check Kafka
        try {
            if (kafkaTemplate != null) {
                kafkaTemplate.send("health-check", "ping", "pong");
                status.put("kafka", "UP");
            } else {
                status.put("kafka", "UNKNOWN");
                status.put("status", "DEGRADED");
            }
        } catch (Exception e) {
            status.put("kafka", "DOWN - " + e.getMessage());
            status.put("status", "DEGRADED");
        }

        // Check PostgreSQL
        try {
            jdbcTemplate.queryForObject("SELECT 1", Integer.class);
            status.put("postgresql", "UP");
        } catch (Exception e) {
            status.put("postgresql", "DOWN - " + e.getMessage());
            status.put("status", "DEGRADED");
        }

        return status;
    }
}
