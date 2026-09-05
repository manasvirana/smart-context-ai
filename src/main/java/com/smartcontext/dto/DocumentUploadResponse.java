package com.smartcontext.dto;

import java.util.UUID;

public class DocumentUploadResponse {

    private UUID documentId;
    private String status;

    public DocumentUploadResponse() {
    }

    public DocumentUploadResponse(UUID documentId, String status) {
        this.documentId = documentId;
        this.status = status;
    }

    public UUID getDocumentId() {
        return documentId;
    }

    public void setDocumentId(UUID documentId) {
        this.documentId = documentId;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
}
