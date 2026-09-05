package com.smartcontext.repository;

import com.smartcontext.model.Chunk;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Repository
public class ChunkRepository {

    private final DatabaseClient db;

    public ChunkRepository(DatabaseClient db) {
        this.db = db;
    }

    public Mono<Void> deleteByDocumentId(UUID documentId) {
        return db.sql("DELETE FROM chunks WHERE document_id = :documentId")
                .bind("documentId", documentId)
                .then();
    }

    public Mono<Void> insert(UUID id, UUID documentId, int index, String content, String vectorLiteral) {
        if (vectorLiteral == null) {
            return db.sql("""
                            INSERT INTO chunks (id, document_id, chunk_index, content)
                            VALUES (:id, :documentId, :chunkIndex, :content)
                            """)
                    .bind("id", id)
                    .bind("documentId", documentId)
                    .bind("chunkIndex", index)
                    .bind("content", content)
                    .then();
        }
        return db.sql("""
                        INSERT INTO chunks (id, document_id, chunk_index, content, embedding)
                        VALUES (:id, :documentId, :chunkIndex, :content, CAST(:embedding AS vector))
                        """)
                .bind("id", id)
                .bind("documentId", documentId)
                .bind("chunkIndex", index)
                .bind("content", content)
                .bind("embedding", vectorLiteral)
                .then();
    }

    public Flux<Chunk> searchByKeyword(String query, int topK, List<UUID> documentIds) {
        boolean filter = documentIds != null && !documentIds.isEmpty();
        String sql = """
                SELECT c.id, c.document_id, c.chunk_index, c.content, d.original_filename,
                       ts_rank(c.content_tsv, plainto_tsquery('english', :query)) AS score
                FROM chunks c
                JOIN documents d ON d.id = c.document_id
                WHERE c.content_tsv @@ plainto_tsquery('english', :query)
                %s
                ORDER BY score DESC
                LIMIT :topK
                """.formatted(filter ? "AND c.document_id = ANY(CAST(:docIds AS uuid[]))" : "");
        var spec = db.sql(sql).bind("query", query).bind("topK", topK);
        if (filter) {
            spec = spec.bind("docIds", pgArray(documentIds));
        }
        return spec.map(this::toChunk).all();
    }

    public Flux<Chunk> searchHybrid(String query, String vectorLiteral, int topK, List<UUID> documentIds) {
        boolean filter = documentIds != null && !documentIds.isEmpty();
        String doc = filter ? "AND document_id = ANY(CAST(:docIds AS uuid[]))" : "";
        String sql = """
                WITH vec AS (
                    SELECT id, ROW_NUMBER() OVER (ORDER BY embedding <=> CAST(:emb AS vector)) AS rank
                    FROM chunks
                    WHERE embedding IS NOT NULL
                    %s
                    ORDER BY embedding <=> CAST(:emb AS vector)
                    LIMIT 20
                ),
                kw AS (
                    SELECT id, ROW_NUMBER() OVER (ORDER BY ts_rank(content_tsv, plainto_tsquery('english', :query)) DESC) AS rank
                    FROM chunks
                    WHERE content_tsv @@ plainto_tsquery('english', :query)
                    %s
                    LIMIT 20
                ),
                merged AS (
                    SELECT COALESCE(v.id, k.id) AS id,
                           COALESCE(1.0 / (60 + v.rank), 0) + COALESCE(1.0 / (60 + k.rank), 0) AS score
                    FROM vec v
                    FULL OUTER JOIN kw k ON v.id = k.id
                )
                SELECT c.id, c.document_id, c.chunk_index, c.content, d.original_filename, m.score
                FROM merged m
                JOIN chunks c ON c.id = m.id
                JOIN documents d ON d.id = c.document_id
                ORDER BY m.score DESC
                LIMIT :topK
                """.formatted(doc, doc);
        var spec = db.sql(sql).bind("emb", vectorLiteral).bind("query", query).bind("topK", topK);
        if (filter) {
            spec = spec.bind("docIds", pgArray(documentIds));
        }
        return spec.map(this::toChunk).all();
    }

    public Flux<Chunk> latest(int topK, List<UUID> documentIds) {
        boolean filter = documentIds != null && !documentIds.isEmpty();
        String sql = """
                SELECT c.id, c.document_id, c.chunk_index, c.content, d.original_filename, 0 AS score
                FROM chunks c
                JOIN documents d ON d.id = c.document_id
                %s
                ORDER BY c.created_at DESC
                LIMIT :topK
                """.formatted(filter ? "WHERE c.document_id = ANY(CAST(:docIds AS uuid[]))" : "");
        var spec = db.sql(sql).bind("topK", topK);
        if (filter) {
            spec = spec.bind("docIds", pgArray(documentIds));
        }
        return spec.map(this::toChunk).all();
    }

    private Chunk toChunk(io.r2dbc.spi.Row row, io.r2dbc.spi.RowMetadata meta) {
        Chunk chunk = new Chunk();
        chunk.setId(row.get("id", UUID.class));
        chunk.setDocumentId(row.get("document_id", UUID.class));
        chunk.setChunkIndex(row.get("chunk_index", Integer.class));
        chunk.setContent(row.get("content", String.class));
        chunk.setFilename(row.get("original_filename", String.class));
        Double score = row.get("score", Double.class);
        chunk.setScore(score == null ? 0 : score);
        return chunk;
    }

    private String pgArray(List<UUID> ids) {
        return ids.stream().map(UUID::toString).collect(Collectors.joining(",", "{", "}"));
    }

    public static String toVectorLiteral(List<Double> values) {
        return values.stream().map(String::valueOf).collect(Collectors.joining(",", "[", "]"));
    }
}
