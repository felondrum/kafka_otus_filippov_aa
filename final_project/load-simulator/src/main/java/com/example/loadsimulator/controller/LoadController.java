package com.example.loadsimulator.controller;

import com.example.loadsimulator.dto.*;
import com.example.loadsimulator.service.LoadSchedulerManager;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/load")
public class LoadController {

    private static final Logger log = LoggerFactory.getLogger(LoadController.class);

    private final LoadSchedulerManager schedulerManager;

    public LoadController(LoadSchedulerManager schedulerManager) {
        this.schedulerManager = schedulerManager;
    }

    @PostMapping("/start")
    public ResponseEntity<?> startLoad(@Valid @RequestBody LoadStartRequest request) {
        try {
            int burstSize = request.getBurstSize() != null ? request.getBurstSize() : 20;
            int fraudPercent = request.getFraudPercent() != null ? request.getFraudPercent() : 15;

            schedulerManager.start(
                    request.getTotalCalls(),
                    request.getDurationMinutes(),
                    burstSize,
                    fraudPercent
            );

            LoadStartResponse response = new LoadStartResponse(
                    true,
                    new LoadStartResponse.Configuration(
                            request.getTotalCalls(),
                            request.getDurationMinutes(),
                            burstSize,
                            fraudPercent
                    )
            );

            return ResponseEntity.status(HttpStatus.ACCEPTED).body(response);

        } catch (Exception e) {
            log.error("Failed to start load generation", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(java.util.Map.of("error", "Failed to start load generation: " + e.getMessage()));
        }
    }

    @PostMapping("/stop")
    public ResponseEntity<?> stopLoad() {
        try {
            if (!schedulerManager.isRunning()) {
                return ResponseEntity.status(HttpStatus.NOT_FOUND)
                        .body(java.util.Map.of("error", "No active load generation"));
            }

            LoadSchedulerManager.StopResult result = schedulerManager.stop();
            int completed = result.completedCalls();
            int remaining = result.remainingCalls();

            LoadStopResponse response = new LoadStopResponse(true, completed, remaining);
            return ResponseEntity.ok(response);

        } catch (Exception e) {
            log.error("Failed to stop load generation", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(java.util.Map.of("error", "Failed to stop load generation: " + e.getMessage()));
        }
    }

    @GetMapping("/status")
    public ResponseEntity<LoadStatusResponse> getStatus() {
        LoadSchedulerManager.Status status = schedulerManager.getStatus();

        LoadStatusResponse response = new LoadStatusResponse(
                status.running(),
                status.totalCalls(),
                status.completedCalls(),
                status.failedCalls(),
                status.percentage()
        );

        return ResponseEntity.ok(response);
    }
}
