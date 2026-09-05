package com.smartcontext.dto;

import java.util.List;
import java.util.UUID;

public class AskRequest {

    private String query;
    private List<UUID> documentIds;

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public List<UUID> getDocumentIds() {
        return documentIds;
    }

    public void setDocumentIds(List<UUID> documentIds) {
        this.documentIds = documentIds;
    }
}
