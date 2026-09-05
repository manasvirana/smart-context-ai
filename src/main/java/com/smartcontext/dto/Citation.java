package com.smartcontext.dto;

import java.util.UUID;

public class Citation {

    private UUID chunkId;
    private UUID documentId;
    private String filename;
    private int chunkIndex;
    private double score;

    public Citation() {
    }

    public Citation(UUID chunkId, UUID documentId, String filename, int chunkIndex, double score) {
        this.chunkId = chunkId;
        this.documentId = documentId;
        this.filename = filename;
        this.chunkIndex = chunkIndex;
        this.score = score;
    }

    public UUID getChunkId() {
        return chunkId;
    }

    public void setChunkId(UUID chunkId) {
        this.chunkId = chunkId;
    }

    public UUID getDocumentId() {
        return documentId;
    }

    public void setDocumentId(UUID documentId) {
        this.documentId = documentId;
    }

    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public int getChunkIndex() {
        return chunkIndex;
    }

    public void setChunkIndex(int chunkIndex) {
        this.chunkIndex = chunkIndex;
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }
}
