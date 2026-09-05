package com.smartcontext.service;

import com.smartcontext.dto.DocumentStatusResponse;
import com.smartcontext.dto.DocumentUploadResponse;
import com.smartcontext.dto.DocumentUploadedEvent;
import com.smartcontext.model.Document;
import com.smartcontext.model.DocumentStatus;
import com.smartcontext.repository.DocumentRepository;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;

@Service
public class DocumentService {

    private static final Set<String> ALLOWED = Set.of(
            ".pdf", ".txt", ".md", ".log", ".java", ".py", ".js", ".ts", ".json"
    );

    private final Path uploadDirectory;
    private final DocumentRepository documents;
    private final IngestProducer ingestProducer;

    public DocumentService(Path uploadDirectory, DocumentRepository documents, IngestProducer ingestProducer) {
        this.uploadDirectory = uploadDirectory;
        this.documents = documents;
        this.ingestProducer = ingestProducer;
    }

    public Mono<DocumentUploadResponse> upload(FilePart filePart) {
        String filename = filePart.filename() == null ? "upload" : filePart.filename();
        checkType(filename);

        UUID id = UUID.randomUUID();
        Path dest = uploadDirectory.resolve(id + "_" + filename.replaceAll("[^a-zA-Z0-9._-]", "_"));
        String contentType = filePart.headers().getContentType() == null
                ? ""
                : filePart.headers().getContentType().toString();

        return filePart.transferTo(dest)
                .then(documents.insert(id, filename, contentType, dest.toString()))
                .then(ingestProducer.publish(new DocumentUploadedEvent(id, dest.toString(), filename, contentType, false)))
                .thenReturn(new DocumentUploadResponse(id, DocumentStatus.ACCEPTED.name()));
    }

    public Mono<DocumentStatusResponse> getStatus(UUID id) {
        return documents.findById(id).map(this::toStatusDto);
    }

    public Mono<DocumentStatusResponse> reindex(UUID id) {
        return documents.findById(id)
                .switchIfEmpty(Mono.error(new NotFoundException("document not found")))
                .flatMap(doc -> documents.updateStatus(id, DocumentStatus.ACCEPTED, null, doc.getChunkCount())
                        .then(ingestProducer.publish(new DocumentUploadedEvent(
                                doc.getId(),
                                doc.getStoragePath(),
                                doc.getOriginalFilename(),
                                doc.getContentType(),
                                true
                        )))
                        .thenReturn(toStatusDto(withStatus(doc, DocumentStatus.ACCEPTED))));
    }

    public Mono<Void> delete(UUID id) {
        return documents.findById(id)
                .switchIfEmpty(Mono.error(new NotFoundException("document not found")))
                .flatMap(doc -> documents.deleteById(id)
                        .then(Mono.fromRunnable(() -> {
                            try {
                                Files.deleteIfExists(Path.of(doc.getStoragePath()));
                            } catch (Exception ignored) {
                                // file cleanup is best-effort
                            }
                        }).subscribeOn(Schedulers.boundedElastic()))
                        .then());
    }

    private Document withStatus(Document doc, DocumentStatus status) {
        doc.setStatus(status);
        return doc;
    }

    private DocumentStatusResponse toStatusDto(Document doc) {
        DocumentStatusResponse dto = new DocumentStatusResponse();
        dto.setDocumentId(doc.getId());
        dto.setFilename(doc.getOriginalFilename());
        dto.setStatus(doc.getStatus().name());
        dto.setChunkCount(doc.getChunkCount());
        dto.setFailureReason(doc.getFailureReason() == null ? "" : doc.getFailureReason());
        return dto;
    }

    private void checkType(String filename) {
        String lower = filename.toLowerCase(Locale.ROOT);
        boolean ok = false;
        for (String ext : ALLOWED) {
            if (lower.endsWith(ext)) {
                ok = true;
                break;
            }
        }
        if (!ok) {
            throw new IllegalArgumentException("file type not supported");
        }
    }

    public static class NotFoundException extends RuntimeException {
        public NotFoundException(String message) {
            super(message);
        }
    }
}
