package com.smartcontext.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.smartcontext.config.AppProperties;
import com.smartcontext.dto.AskRequest;
import com.smartcontext.dto.Citation;
import com.smartcontext.model.Chunk;
import com.smartcontext.repository.ChunkRepository;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Service
public class AskService {

    private static final String SYSTEM = """
            Answer only from the context.
            If the context does not contain the answer, say you do not know.
            """;

    private final ChunkRepository chunks;
    private final OpenAiService openAi;
    private final AppProperties properties;
    private final ObjectMapper objectMapper;
    private final AppMetrics metrics;

    public AskService(
            ChunkRepository chunks,
            OpenAiService openAi,
            AppProperties properties,
            ObjectMapper objectMapper,
            AppMetrics metrics
    ) {
        this.chunks = chunks;
        this.openAi = openAi;
        this.properties = properties;
        this.objectMapper = objectMapper;
        this.metrics = metrics;
    }

    public Flux<ServerSentEvent<String>> ask(AskRequest request) {
        if (request == null || request.getQuery() == null || request.getQuery().isBlank()) {
            return Flux.just(sse("error", "query is required"));
        }
        if (!properties.getOpenai().configured()) {
            return Flux.just(sse("error", "OPENAI_API_KEY is not set"));
        }

        String query = request.getQuery().trim();
        List<UUID> docIds = request.getDocumentIds();
        Flux<ServerSentEvent<String>> work = openAi.embed(List.of(query))
                .onErrorReturn(List.of())
                .flatMapMany(vectors -> {
                    if (vectors.isEmpty()) {
                        metrics.retrieval("keyword");
                        return chunks.searchByKeyword(query, 8, docIds)
                                .switchIfEmpty(chunks.latest(8, docIds)
                                        .doOnSubscribe(s -> metrics.retrieval("latest")));
                    }
                    metrics.retrieval("hybrid");
                    String emb = ChunkRepository.toVectorLiteral(vectors.get(0));
                    return chunks.searchHybrid(query, emb, 8, docIds)
                            .switchIfEmpty(chunks.searchByKeyword(query, 8, docIds)
                                    .doOnSubscribe(s -> metrics.retrieval("keyword")))
                            .switchIfEmpty(chunks.latest(8, docIds)
                                    .doOnSubscribe(s -> metrics.retrieval("latest")));
                })
                .collectList()
                .flatMapMany(hits -> {
                    if (hits.isEmpty()) {
                        return Flux.just(
                                sse("token", "I do not know. Nothing relevant was found in the documents."),
                                sse("done", "insufficient-context")
                        );
                    }
                    Flux<ServerSentEvent<String>> answer = openAi.streamChat(SYSTEM, userPrompt(query, hits))
                            .map(t -> sse("token", t))
                            .onErrorResume(e -> Flux.just(sse("token",
                                    "OpenAI is rate-limited or unavailable. Retrieved passages:\n"
                                            + fallbackText(hits))));
                    return Flux.concat(
                            Flux.just(sse("citation", citations(hits))),
                            answer,
                            Flux.just(sse("done", "ok"))
                    );
                })
                .onErrorResume(e -> Flux.just(sse("error", e.getMessage() == null ? "ask failed" : e.getMessage())))
                .timeout(Duration.ofSeconds(40), Flux.just(sse("error", "timed out waiting for OpenAI")));

        return Flux.concat(Flux.just(sse("status", "searching")), work);
    }

    private String userPrompt(String query, List<Chunk> hits) {
        StringBuilder sb = new StringBuilder();
        sb.append("Question:\n").append(query).append("\n\nContext:\n");
        for (Chunk chunk : hits) {
            sb.append("- ").append(chunk.getFilename() == null ? "" : chunk.getFilename())
                    .append(" #").append(chunk.getChunkIndex())
                    .append(" (").append(chunk.getId()).append(")\n")
                    .append(chunk.getContent()).append("\n\n");
        }
        return sb.toString();
    }

    private String fallbackText(List<Chunk> hits) {
        StringBuilder sb = new StringBuilder();
        for (Chunk chunk : hits) {
            sb.append(chunk.getFilename() == null ? "" : chunk.getFilename())
                    .append(" #").append(chunk.getChunkIndex()).append("\n")
                    .append(chunk.getContent()).append("\n---\n");
        }
        return sb.toString();
    }

    private String citations(List<Chunk> hits) {
        List<Citation> list = new ArrayList<>();
        for (Chunk chunk : hits) {
            list.add(new Citation(
                    chunk.getId(),
                    chunk.getDocumentId(),
                    chunk.getFilename() == null ? "" : chunk.getFilename(),
                    chunk.getChunkIndex(),
                    chunk.getScore()
            ));
        }
        try {
            return objectMapper.writeValueAsString(list);
        } catch (JsonProcessingException e) {
            return "[]";
        }
    }

    private ServerSentEvent<String> sse(String event, String data) {
        return ServerSentEvent.<String>builder().event(event).data(data).build();
    }
}
