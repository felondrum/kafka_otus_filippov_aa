package com.example.reportingnps.controller;

import com.example.reportingnps.dto.PageResponse;
import com.example.reportingnps.entity.CallMetadata;
import com.example.reportingnps.repository.CallMetadataRepository;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
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
@Tag(name = "Call Metadata", description = "API для получения метаданных звонков")
public class MetadataController {

    private final CallMetadataRepository callMetadataRepository;

    public MetadataController(CallMetadataRepository callMetadataRepository) {
        this.callMetadataRepository = callMetadataRepository;
    }

    @GetMapping("/{callId}")
    @Operation(
            summary = "Получить метаданные звонка",
            description = "Возвращает полную информацию о звонке по его UUID."
    )
    @ApiResponse(responseCode = "200", description = "Метаданные найдены")
    @ApiResponse(responseCode = "404", description = "Звонок не найден")
    public ResponseEntity<CallMetadata> getCallMetadata(
            @Parameter(description = "UUID звонка", required = true)
            @PathVariable UUID callId) {
        return callMetadataRepository.findByCallId(callId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.status(HttpStatus.NOT_FOUND).build());
    }

    @GetMapping("/status/{status}")
    @Operation(
            summary = "Получить звонки по статусу",
            description = "Возвращает пагинированный список звонков с указанным статусом."
    )
    @ApiResponse(responseCode = "200", description = "Список звонков получен")
    public ResponseEntity<PageResponse<CallMetadata>> getCallsByStatus(
            @Parameter(description = "Статус звонка", required = true)
            @PathVariable String status,
            @Parameter(description = "Номер страницы (0-based)", required = true)
            @RequestParam(defaultValue = "0") int page,
            @Parameter(description = "Размер страницы (макс. 200)", required = true)
            @RequestParam(defaultValue = "50") int size) {

        // Enforce max page size
        if (size > 200) {
            size = 200;
        }

        // Get all calls with the given status (large batch for pagination)
        var allCalls = callMetadataRepository.findByCallStatus(status, PageRequest.of(0, size * 1000));
        long totalElements = allCalls.size();
        int totalPages = (int) Math.ceil((double) totalElements / size);

        int fromIndex = page * size;
        int toIndex = Math.min(fromIndex + size, (int) totalElements);

        var content = fromIndex < totalElements ?
                allCalls.subList(fromIndex, toIndex) :
                java.util.Collections.<CallMetadata>emptyList();

        PageResponse<CallMetadata> response = new PageResponse<>(
                content, page, size, totalElements, totalPages);

        return ResponseEntity.ok(response);
    }
}
