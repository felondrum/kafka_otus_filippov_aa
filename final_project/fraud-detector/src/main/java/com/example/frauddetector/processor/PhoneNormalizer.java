package com.example.frauddetector.processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Normalizes phone numbers to E.164 format (+7XXXXXXXXXX).
 * Handles various input formats:
 * - 8XXXXXXXXXX -> +7XXXXXXXXXX
 * - 7XXXXXXXXXX -> +7XXXXXXXXXX
 * - +7XXXXXXXXXX -> +7XXXXXXXXXX
 * - +8XXXXXXXXXX -> +7XXXXXXXXXX
 * - 8(XXX)XXX-XX-XX -> +7XXXXXXXXXX
 * - +7(XXX)XXX-XX-XX -> +7XXXXXXXXXX
 */
public class PhoneNormalizer {

    private static final Logger log = LoggerFactory.getLogger(PhoneNormalizer.class);
    private static final int RUSSIAN_MOBILE_PREFIX = 7;
    private static final int RUSSIAN_NATIONAL_PREFIX = 8;

    /**
     * Normalize phone number to E.164 format.
     *
     * @param phone raw phone number string
     * @return normalized E.164 phone number (+7XXXXXXXXXX)
     */
    public String normalize(String phone) {
        if (phone == null || phone.trim().isEmpty()) {
            log.warn("Received null or empty phone number");
            return phone;
        }

        // Strip all non-digit characters
        String digits = phone.replaceAll("\\D", "");

        // Handle leading digits
        if (digits.startsWith("8") && digits.length() == 11) {
            // Replace national prefix 8 with international prefix 7
            digits = "7" + digits.substring(1);
        } else if (digits.startsWith("7") && digits.length() == 11) {
            // Already has international prefix
            digits = "7" + digits.substring(1);
        } else if (digits.startsWith("8") && digits.length() == 10) {
            // 10 digits starting with 8 (without country code)
            digits = "7" + digits;
        } else if (digits.length() == 10) {
            // 10 digits without country code
            digits = "7" + digits;
        }

        // Validate length
        if (digits.length() != 11 || !digits.startsWith("7")) {
            log.warn("Cannot normalize phone number: '{}', result: '{}'", phone, digits);
        }

        return "+" + digits;
    }
}
