package com.smartcontext.dto;

import java.util.UUID;

public class DocumentUploadedEvent {

    private UUID documentId;
    private String storagePath;
    private String originalFilename;
    private String contentType;
    private boolean reindex;

    public DocumentUploadedEvent() {
    }

    public DocumentUploadedEvent(UUID documentId, String storagePath, String originalFilename, String contentType) {
        this(documentId, storagePath, originalFilename, contentType, false);
    }

    public DocumentUploadedEvent(
            UUID documentId,
            String storagePath,
            String originalFilename,
            String contentType,
            boolean reindex
    ) {
        this.documentId = documentId;
        this.storagePath = storagePath;
        this.originalFilename = originalFilename;
        this.contentType = contentType;
        this.reindex = reindex;
    }

    public UUID getDocumentId() {
        return documentId;
    }

    public void setDocumentId(UUID documentId) {
        this.documentId = documentId;
    }

    public String getStoragePath() {
        return storagePath;
    }

    public void setStoragePath(String storagePath) {
        this.storagePath = storagePath;
    }

    public String getOriginalFilename() {
        return originalFilename;
    }

    public void setOriginalFilename(String originalFilename) {
        this.originalFilename = originalFilename;
    }

    public String getContentType() {
        return contentType;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    public boolean isReindex() {
        return reindex;
    }

    public void setReindex(boolean reindex) {
        this.reindex = reindex;
    }
}
