package com.example.loadsimulator.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PhoneGeneratorTest {

    private PhoneGenerator phoneGenerator;

    @BeforeEach
    void setUp() {
        phoneGenerator = new PhoneGenerator();
    }

    @Test
    void size_returnsFiftyPhones() {
        assertEquals(50, phoneGenerator.size());
    }

    @Test
    void generateRandomPhone_returnsE164Format() {
        String phone = phoneGenerator.generateRandomPhone();

        assertNotNull(phone);
        assertTrue(phone.matches("^\\+79\\d{9}$"),
                "Phone must match +7XXXXXXXXXX format");
    }

    @Test
    void generateRandomPhone_hasDiversity() {
        java.util.Set<String> phones = new java.util.HashSet<>();
        for (int i = 0; i < 100; i++) {
            phones.add(phoneGenerator.generateRandomPhone());
        }

        // With 50 phones and 100 draws, we should get multiple unique phones
        assertTrue(phones.size() > 10,
                "Should generate diverse phone numbers, got only " + phones.size() + " unique");
    }

    @Test
    void generateFrequentCallPhone_returnsFromFirstThree() {
        String phone = phoneGenerator.generateFrequentCallPhone();

        assertTrue(phone.matches("^\\+7900\\d{7}$") || phone.matches("^\\+7901\\d{7}$") || phone.matches("^\\+7902\\d{7}$"),
                "Frequent call phone should be from first 3 numbers");
    }

    @Test
    void generateNpsEscalationPhone_returnsFromPool() {
        String phone = phoneGenerator.generateNpsEscalationPhone();

        assertNotNull(phone);
        assertTrue(phone.matches("^\\+79\\d{9}$"),
                "NPS escalation phone must be valid E.164");
    }
}
