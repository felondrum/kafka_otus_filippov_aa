package com.example.frauddetector.processor;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive unit tests for PhoneNormalizer.
 */
class PhoneNormalizerUnitTest {

    private final PhoneNormalizer normalizer = new PhoneNormalizer();

    @ParameterizedTest
    @CsvSource({
        "'89991234567', '+79991234567'",
        "'+79991234567', '+79991234567'",
        "'9991234567', '+79991234567'",
        "'8(999)123-45-67', '+79991234567'",
        "'8 999 123-45-67', '+79991234567'",
        "'+7 (999) 123-45-67', '+79991234567'",
        "'79991234567', '+79991234567'",
    })
    void shouldNormalizeVariousPhoneFormats(String input, String expected) {
        assertEquals(expected, normalizer.normalize(input));
    }

    @Test
    void shouldHandleEmptyString() {
        assertEquals("", normalizer.normalize(""));
    }

    @Test
    void shouldHandleNull() {
        assertEquals("", normalizer.normalize(null));
    }

    @Test
    void shouldHandleInvalidFormat() {
        assertEquals("", normalizer.normalize("invalid"));
        assertEquals("", normalizer.normalize("abc"));
        assertEquals("", normalizer.normalize("123"));
    }

    @Test
    void shouldHandleAlreadyNormalized() {
        assertEquals("+79991234567", normalizer.normalize("+79991234567"));
        assertEquals("+14155552671", normalizer.normalize("+14155552671"));
    }

    @Test
    void shouldHandleInternationalNumbers() {
        assertEquals("+14155552671", normalizer.normalize("14155552671"));
        assertEquals("+442071234567", normalizer.normalize("442071234567"));
    }
}
