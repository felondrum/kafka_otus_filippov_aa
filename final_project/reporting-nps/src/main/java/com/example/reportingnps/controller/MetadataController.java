package com.example.reportingnps.controller;

import com.example.reportingnps.dto.PageResponse;
import com.example.reportingnps.entity.CallMetadata;
import com.example.reportingnps.repository.CallMetadataRepository;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
@RequestMapping("/api/metadata")
public class MetadataController {

    private final CallMetadataRepository callMetadataRepository;

    public MetadataController(CallMetadataRepository callMetadataRepository) {
        this.callMetadataRepository = callMetadataRepository;
    }

    @GetMapping("/{callId}")
    public ResponseEntity<CallMetadata> getCallMetadata(@PathVariable UUID callId) {
        return callMetadataRepository.findByCallId(callId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.status(HttpStatus.NOT_FOUND).build());
    }

    @GetMapping("/status/{status}")
    public ResponseEntity<PageResponse<CallMetadata>> getCallsByStatus(
            @PathVariable String status,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "50") int size) {

        // Enforce max page size
        if (size > 200) {
            size = 200;
        }

        Pageable pageable = PageRequest.of(page, size, Sort.by("callStartTime").descending());
        Page<CallMetadata> callPage = callMetadataRepository.findAll(
                org.springframework.data.jpa.repository.JpaRepository<CallMetadata, UUID>
                        .super::findAll, pageable);

        // Manual pagination since we need to filter by status
        var allCalls = callMetadataRepository.findByCallStatus(status, PageRequest.of(0, size * 1000));
        long totalElements = allCalls.size();
        int totalPages = (int) Math.ceil((double) totalElements / size);

        int fromIndex = page * size;
        int toIndex = Math.min(fromIndex + size, totalElements);

        var content = fromIndex < totalElements ?
                allCalls.subList(fromIndex, toIndex) :
                java.util.Collections.emptyList();

        PageResponse<CallMetadata> response = new PageResponse<>(
                content, page, size, totalElements, totalPages);

        return ResponseEntity.ok(response);
    }
}
