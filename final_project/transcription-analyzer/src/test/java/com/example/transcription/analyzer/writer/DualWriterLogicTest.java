package com.example.transcription.analyzer.writer;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DualWriterLogicTest {

    @Test
    void testRetryConstants() {
        assertEquals(3, DualWriter.MAX_RETRIES);
        assertEquals(1000, DualWriter.INITIAL_BACKOFF_MS);
    }
}
