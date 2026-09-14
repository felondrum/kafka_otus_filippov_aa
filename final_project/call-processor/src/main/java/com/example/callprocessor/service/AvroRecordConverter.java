package com.example.callprocessor.service;

import com.example.callprocessor.dto.CallEventRequest;
import com.example.callprocessor.avro.CallEvent;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Component
public class AvroRecordConverter {

    public CallEvent toAvroRecord(CallEventRequest request, String status) {
        CallEvent avroEvent = new CallEvent();
        avroEvent.setCallId(request.getCallId());
        avroEvent.setPhone(request.getPhone());
        avroEvent.setDuration(request.getDuration());
        avroEvent.setAgentId(request.getAgentId());
        avroEvent.setNpsScore(request.getNpsScore());
        avroEvent.setStatus(status);
        avroEvent.setTimestamp(Instant.now().toString());
        return avroEvent;
    }

    public CallEvent toCompletedRecord(CallEventRequest request) {
        return toAvroRecord(request, null);
    }

    public CallEvent toMetadataRecord(CallEventRequest request) {
        return toAvroRecord(request, "PENDING");
    }
}
