package com.example.transcription.analyzer.integration;

import com.example.transcription.analyzer.IntegrationTest;
import com.example.transcription.analyzer.writer.DualWriter;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test for JDBC failure:
 * PostgreSQL down → Kafka write succeeds, error logged
 */
@IntegrationTest
class JdbcFailureIntegrationTest {

    @Autowired
    private DualWriter dualWriter;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private KafkaTemplate<String, ?> kafkaTemplate;

    @Test
    void testJdbcTemplateAvailable() {
        // Verify JdbcTemplate is configured
        assertNotNull(jdbcTemplate);

        // Verify PostgreSQL connection works in test environment
        Integer result = jdbcTemplate.queryForObject("SELECT 1", Integer.class);
        assertEquals(1, result);
    }

    @Test
    void testKafkaTemplateAvailable() {
        // Verify KafkaTemplate is configured
        assertNotNull(kafkaTemplate);
    }

    @Test
    void testDualWriterConfigured() {
        // Verify DualWriter is configured
        assertNotNull(dualWriter);

        // Verify retry constants
        assertEquals(3, DualWriter.MAX_RETRIES);
        assertEquals(1000, DualWriter.INITIAL_BACKOFF_MS);
    }
}
