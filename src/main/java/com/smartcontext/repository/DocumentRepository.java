package com.smartcontext.repository;

import com.smartcontext.model.Document;
import com.smartcontext.model.DocumentStatus;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

import java.time.OffsetDateTime;
import java.util.UUID;

@Repository
public class DocumentRepository {

    private final DatabaseClient db;

    public DocumentRepository(DatabaseClient db) {
        this.db = db;
    }

    public Mono<Void> insert(UUID id, String filename, String contentType, String storagePath) {
        return db.sql("""
                        INSERT INTO documents (id, original_filename, content_type, storage_path, status)
                        VALUES (:id, :filename, :contentType, :storagePath, :status)
                        """)
                .bind("id", id)
                .bind("filename", filename)
                .bind("contentType", contentType == null ? "" : contentType)
                .bind("storagePath", storagePath)
                .bind("status", DocumentStatus.ACCEPTED.name())
                .then();
    }

    public Mono<Document> findById(UUID id) {
        return db.sql("SELECT * FROM documents WHERE id = :id")
                .bind("id", id)
                .map((row, meta) -> {
                    Document doc = new Document();
                    doc.setId(row.get("id", UUID.class));
                    doc.setOriginalFilename(row.get("original_filename", String.class));
                    doc.setContentType(row.get("content_type", String.class));
                    doc.setStoragePath(row.get("storage_path", String.class));
                    doc.setStatus(DocumentStatus.valueOf(row.get("status", String.class)));
                    doc.setFailureReason(row.get("failure_reason", String.class));
                    Integer chunks = row.get("chunk_count", Integer.class);
                    doc.setChunkCount(chunks == null ? 0 : chunks);
                    OffsetDateTime created = row.get("created_at", OffsetDateTime.class);
                    OffsetDateTime updated = row.get("updated_at", OffsetDateTime.class);
                    if (created != null) {
                        doc.setCreatedAt(created.toInstant());
                    }
                    if (updated != null) {
                        doc.setUpdatedAt(updated.toInstant());
                    }
                    return doc;
                })
                .one();
    }

    public Mono<Void> updateStatus(UUID id, DocumentStatus status, String failureReason, Integer chunkCount) {
        var spec = db.sql("""
                        UPDATE documents
                        SET status = :status,
                            failure_reason = :failureReason,
                            chunk_count = COALESCE(:chunkCount, chunk_count),
                            updated_at = now()
                        WHERE id = :id
                        """)
                .bind("id", id)
                .bind("status", status.name());
        if (failureReason == null) {
            spec = spec.bindNull("failureReason", String.class);
        } else {
            spec = spec.bind("failureReason", failureReason);
        }
        if (chunkCount == null) {
            spec = spec.bindNull("chunkCount", Integer.class);
        } else {
            spec = spec.bind("chunkCount", chunkCount);
        }
        return spec.then();
    }

    public Mono<Boolean> claimForProcessing(UUID id, boolean reindex) {
        String allowed = reindex
                ? "'READY','FAILED','ACCEPTED'"
                : "'ACCEPTED','FAILED'";
        return db.sql("""
                        UPDATE documents
                        SET status = :status,
                            failure_reason = NULL,
                            updated_at = now()
                        WHERE id = :id
                          AND status IN (%s)
                        """.formatted(allowed))
                .bind("status", DocumentStatus.CHUNKING.name())
                .bind("id", id)
                .fetch()
                .rowsUpdated()
                .map(n -> n > 0);
    }

    public Mono<Long> deleteById(UUID id) {
        return db.sql("DELETE FROM documents WHERE id = :id")
                .bind("id", id)
                .fetch()
                .rowsUpdated();
    }
}
