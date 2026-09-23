package com.example.transcription.analyzer.controller;

import com.example.transcription.analyzer.processor.CallMetadataProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/calls")
public class CallController {

    private static final Logger log = LoggerFactory.getLogger(CallController.class);

    private final CallMetadataProcessor callMetadataProcessor;

    public CallController(CallMetadataProcessor callMetadataProcessor) {
        this.callMetadataProcessor = callMetadataProcessor;
    }

    /**
     * Process a completed call event.
     * Triggers the full pipeline: transcription -> summary -> enrichment -> dual-write.
     */
    @PostMapping("/process")
    public ResponseEntity<Map<String, String>> processCall(@RequestBody Map<String, Object> request) {
        String callId = (String) request.get("callId");
        double durationMinutes = Double.parseDouble(request.getOrDefault("durationMinutes", "5.0").toString());
        String phone = (String) request.getOrDefault("phone", "unknown");

        if (callId == null || callId.isEmpty()) {
            return ResponseEntity.badRequest().body(Map.of("error", "callId is required"));
        }

        callMetadataProcessor.processCall(callId, durationMinutes, phone);

        return ResponseEntity.accepted().body(Map.of(
                "callId", callId,
                "status", "processing"
        ));
    }
}
