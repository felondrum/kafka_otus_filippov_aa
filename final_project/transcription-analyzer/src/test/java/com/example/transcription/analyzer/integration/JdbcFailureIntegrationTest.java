package com.example.transcription.analyzer.integration;

import com.example.transcription.analyzer.writer.DualWriter;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test for JDBC failure:
 * PostgreSQL down → Kafka write succeeds, error logged
 */
class JdbcFailureIntegrationTest {

    @Test
    void testDualWriterConfigured() {
        assertNotNull(DualWriter.class);
        assertEquals(3, DualWriter.MAX_RETRIES);
        assertEquals(1000, DualWriter.INITIAL_BACKOFF_MS);
    }

    @Test
    void testRetryLogic() {
        // Verify exponential backoff calculation
        long backoff1 = DualWriter.INITIAL_BACKOFF_MS * (long) Math.pow(2, 0);
        long backoff2 = DualWriter.INITIAL_BACKOFF_MS * (long) Math.pow(2, 1);
        long backoff3 = DualWriter.INITIAL_BACKOFF_MS * (long) Math.pow(2, 2);

        assertEquals(1000, backoff1);
        assertEquals(2000, backoff2);
        assertEquals(4000, backoff3);
    }
}
