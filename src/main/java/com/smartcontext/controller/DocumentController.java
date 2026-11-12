package com.smartcontext.controller;

import com.smartcontext.dto.DocumentStatusResponse;
import com.smartcontext.dto.DocumentUploadResponse;
import com.smartcontext.service.DocumentService;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.util.UUID;

@RestController
public class DocumentController {

    private final DocumentService documentService;

    public DocumentController(DocumentService documentService) {
        this.documentService = documentService;
    }

    @PostMapping(value = "/v1/documents", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public Mono<DocumentUploadResponse> upload(@RequestPart("file") FilePart file) {
        return documentService.upload(file);
    }

    @GetMapping("/v1/documents/{id}")
    public Mono<ResponseEntity<DocumentStatusResponse>> status(@PathVariable UUID id) {
        return documentService.getStatus(id)
                .map(ResponseEntity::ok)
                .defaultIfEmpty(ResponseEntity.notFound().build());
    }

    @PostMapping("/v1/documents/{id}/reindex")
    public Mono<DocumentStatusResponse> reindex(@PathVariable UUID id) {
        return documentService.reindex(id);
    }

    @DeleteMapping("/v1/documents/{id}")
    public Mono<ResponseEntity<Void>> delete(@PathVariable UUID id) {
        return documentService.delete(id)
                .thenReturn(ResponseEntity.status(HttpStatus.NO_CONTENT).<Void>build());
    }
}
