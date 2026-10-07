package com.example.callprocessor.controller;

import com.example.callprocessor.dto.CallEventRequest;
import com.example.callprocessor.service.CallEventService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
@RequestMapping("/api")
@Validated
public class CallController {

    private static final Logger log = LoggerFactory.getLogger(CallController.class);

    private final CallEventService callEventService;

    public CallController(CallEventService callEventService) {
        this.callEventService = callEventService;
    }

    @PostMapping("/calls")
    public ResponseEntity<CallEventResponse> createCallEvent(@Valid @RequestBody CallEventRequest request) {
        String correlationId = UUID.randomUUID().toString();
        log.info("Received call event request: callId={}, correlationId={}", request.getCallId(), correlationId);

        callEventService.processCallEvent(request, correlationId);

        CallEventResponse response = new CallEventResponse();
        response.setCorrelationId(correlationId);
        response.setMessage("Call event accepted");
        return ResponseEntity.accepted().body(response);
    }

    @GetMapping("/health")
    public ResponseEntity<HealthResponse> health() {
        try {
            boolean healthy = callEventService.checkKafkaConnectivity();
            if (healthy) {
                HealthResponse response = new HealthResponse();
                response.setStatus("UP");
                return ResponseEntity.ok(response);
            } else {
                HealthResponse response = new HealthResponse();
                response.setStatus("DOWN");
                response.setDetails("Kafka is unreachable");
                return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(response);
            }
        } catch (Exception e) {
            HealthResponse response = new HealthResponse();
            response.setStatus("DOWN");
            response.setDetails("Kafka connection error: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(response);
        }
    }

    public static class CallEventResponse {
        private String correlationId;
        private String message;

        public String getCorrelationId() {
            return correlationId;
        }

        public void setCorrelationId(String correlationId) {
            this.correlationId = correlationId;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }
    }

    public static class HealthResponse {
        private String status;
        private String details;

        public String getStatus() {
            return status;
        }

        public void setStatus(String status) {
            this.status = status;
        }

        public String getDetails() {
            return details;
        }

        public void setDetails(String details) {
            this.details = details;
        }
    }
}
