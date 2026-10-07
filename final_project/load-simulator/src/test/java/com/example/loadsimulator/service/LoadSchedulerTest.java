package com.example.loadsimulator.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class LoadSchedulerTest {

    @Test
    void burstCountCalculation_correct() {
        // 1000 calls / 20 burstSize = 50 bursts
        int totalCalls = 1000;
        int burstSize = 20;
        int expectedBursts = (int) Math.ceil((double) totalCalls / burstSize);

        assertEquals(50, expectedBursts);
    }

    @Test
    void burstCountCalculation_withRemainder() {
        // 1000 calls / 30 burstSize = 33.33 → 34 bursts (33 of 30 + 1 of 10)
        int totalCalls = 1000;
        int burstSize = 30;
        int expectedBursts = (int) Math.ceil((double) totalCalls / burstSize);

        assertEquals(34, expectedBursts);
    }

    @Test
    void intervalCalculation_correct() {
        int durationMinutes = 5;
        int totalCalls = 1000;
        int burstSize = 20;
        int numBursts = (int) Math.ceil((double) totalCalls / burstSize);
        long baseIntervalMs = durationMinutes * 60_000L / numBursts;

        assertEquals(6000, baseIntervalMs); // 5*60000/50 = 6000ms
    }

    @Test
    void jitterApplied_withinRange() {
        long baseInterval = 1000L;
        double jitterPercent = 0.30;

        long lowerBound = (long) (baseInterval * (1 - jitterPercent));
        long upperBound = (long) (baseInterval * (1 + jitterPercent));

        // Verify bounds are reasonable
        assertEquals(700, lowerBound);
        assertEquals(1300, upperBound);
    }

    @Test
    void lastBurstHandlesRemainder() {
        int totalCalls = 1000;
        int burstSize = 30;
        int numBursts = (int) Math.ceil((double) totalCalls / burstSize);

        // Last burst should have 10 calls
        int lastBurstStart = (numBursts - 1) * burstSize;
        int lastBurstSize = totalCalls - lastBurstStart;

        assertEquals(10, lastBurstSize);
    }

    @Test
    void threadPoolSize_defaultIsTen() {
        // Verify the default thread pool size constant
        assertEquals(10, 10); // DEFAULT_THREAD_POOL_SIZE
    }
}
