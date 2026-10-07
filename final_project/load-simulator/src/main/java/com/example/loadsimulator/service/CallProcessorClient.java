package com.example.loadsimulator.service;

import com.example.loadsimulator.dto.CallEventRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Sends call events to call-processor via HTTP POST (fire-and-forget).
 */
@Service
public class CallProcessorClient {

    private static final Logger log = LoggerFactory.getLogger(CallProcessorClient.class);

    private final String callProcessorUrl;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;

    public CallProcessorClient(
            @org.springframework.beans.factory.annotation.Value("${call-processor.url:http://localhost:8081}") String callProcessorUrl,
            ObjectMapper objectMapper) {
        this.callProcessorUrl = callProcessorUrl + "/api/calls";
        this.objectMapper = objectMapper;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
    }

    /**
     * Sends a call event to call-processor in fire-and-forget mode.
     * Does not wait for response.
     */
    public void sendCall(CallEventRequest callEvent) {
        try {
            String jsonBody = objectMapper.writeValueAsString(callEvent);

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(java.net.URI.create(callProcessorUrl))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
                    .build();

            httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                    .thenAccept(response -> {
                        if (response.statusCode() >= 200 && response.statusCode() < 300) {
                            // Success - fire and forget, don't block
                        } else {
                            log.warn("Call-processor returned status {}: {}", response.statusCode(), response.body());
                        }
                    })
                    .exceptionally(throwable -> {
                        log.warn("Failed to send call to call-processor: {}", throwable.getMessage());
                        return null;
                    });

        } catch (Exception e) {
            log.warn("Failed to serialize or prepare call event: {}", e.getMessage());
        }
    }
}
