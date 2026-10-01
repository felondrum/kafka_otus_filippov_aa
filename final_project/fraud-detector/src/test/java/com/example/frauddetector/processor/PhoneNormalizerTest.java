package com.example.frauddetector.processor;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class PhoneNormalizerTest {

    private PhoneNormalizer normalizer = new PhoneNormalizer();

    @Test
    void shouldNormalizeRussianNumberWith8Prefix() {
        assertEquals("+79991234567", normalizer.normalize("89991234567"));
    }

    @Test
    void shouldNormalizeRussianNumberWith7Prefix() {
        assertEquals("+79991234567", normalizer.normalize("79991234567"));
    }

    @Test
    void shouldNormalizeRussianNumberWithPlus7Prefix() {
        assertEquals("+79991234567", normalizer.normalize("+79991234567"));
    }

    @Test
    void shouldNormalizeRussianNumberWithPlus8Prefix() {
        assertEquals("+79991234567", normalizer.normalize("+89991234567"));
    }

    @Test
    void shouldNormalize10DigitNumber() {
        assertEquals("+79991234567", normalizer.normalize("9991234567"));
    }

    @Test
    void shouldNormalizeNumberWithFormatting() {
        assertEquals("+79991234567", normalizer.normalize("8(999)123-45-67"));
        assertEquals("+79991234567", normalizer.normalize("+7(999)123-45-67"));
    }

    @Test
    void shouldHandleNullInput() {
        assertNull(normalizer.normalize(null));
    }

    @Test
    void shouldHandleEmptyInput() {
        String result = normalizer.normalize("");
        assertNotNull(result);
    }

    @Test
    void shouldHandleInvalidPhone() {
        String result = normalizer.normalize("123");
        assertNotNull(result);
    }
}
