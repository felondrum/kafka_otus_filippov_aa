package com.example.transcription.analyzer.enrichment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class CustomerProfileManager {

    private static final Logger log = LoggerFactory.getLogger(CustomerProfileManager.class);

    private final Map<String, Map<String, String>> profileMap = new ConcurrentHashMap<>();

    /**
     * Load customer profile from Kafka event.
     * Called by Kafka listener for customers.profile topic.
     */
    public void loadProfile(String phone, String segment, String riskLevel) {
        Map<String, String> profile = Map.of("phone", phone, "segment", segment, "riskLevel", riskLevel);
        profileMap.put(phone, profile);
        log.debug("Loaded customer profile: phone={}, segment={}, riskLevel={}",
                phone, segment, riskLevel);
    }

    /**
     * Look up customer profile by phone number.
     * Returns null if not found.
     */
    public Map<String, String> lookup(String phone) {
        return profileMap.get(phone);
    }

    /**
     * Get all loaded profiles (for testing).
     */
    public Map<String, Map<String, String>> getAllProfiles() {
        return Map.copyOf(profileMap);
    }

    /**
     * Get the number of loaded profiles (for testing).
     */
    public int getProfileCount() {
        return profileMap.size();
    }

    /**
     * Clear all profiles (for testing).
     */
    public void clear() {
        profileMap.clear();
    }
}
