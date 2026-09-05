package com.smartcontext.service;

import com.smartcontext.config.AppProperties;
import com.smartcontext.dto.DocumentUploadedEvent;
import com.smartcontext.model.DocumentStatus;
import com.smartcontext.repository.ChunkRepository;
import com.smartcontext.repository.DocumentRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

@Service
public class IngestService {

    private static final Logger log = LoggerFactory.getLogger(IngestService.class);
    private static final int CHUNK_SIZE = 1200;
    private static final int OVERLAP = 200;

    private final DocumentRepository documents;
    private final ChunkRepository chunks;
    private final DocumentParserService parser;
    private final OpenAiService openAi;
    private final AppProperties properties;
    private final AppMetrics metrics;

    public IngestService(
            DocumentRepository documents,
            ChunkRepository chunks,
            DocumentParserService parser,
            OpenAiService openAi,
            AppProperties properties,
            AppMetrics metrics
    ) {
        this.documents = documents;
        this.chunks = chunks;
        this.parser = parser;
        this.openAi = openAi;
        this.properties = properties;
        this.metrics = metrics;
    }

    public Mono<Void> handle(DocumentUploadedEvent event) {
        UUID id = event.getDocumentId();
        return documents.claimForProcessing(id, event.isReindex())
                .flatMap(claimed -> {
                    if (!claimed) {
                        log.info("skip duplicate ingest for {}", id);
                        metrics.ingestSkipped();
                        return Mono.empty();
                    }
                    return process(event)
                            .retryWhen(Retry.backoff(3, Duration.ofSeconds(1))
                                    .maxBackoff(Duration.ofSeconds(8))
                                    .filter(e -> !(e instanceof IllegalStateException))
                                    .transientErrors(true))
                            .doOnSuccess(n -> metrics.ingestSuccess())
                            .then()
                            .onErrorResume(e -> {
                                log.error("ingest failed for {}", id, e);
                                metrics.ingestFailed();
                                String msg = e.getMessage() == null ? "ingest failed" : e.getMessage();
                                return documents.updateStatus(id, DocumentStatus.FAILED, msg, null);
                            });
                });
    }

    private Mono<Integer> process(DocumentUploadedEvent event) {
        UUID id = event.getDocumentId();
        return parser.extractText(
                        Path.of(event.getStoragePath()),
                        event.getOriginalFilename(),
                        event.getContentType()
                )
                .flatMap(text -> {
                    List<String> pieces = parser.chunk(text, CHUNK_SIZE, OVERLAP);
                    if (pieces.isEmpty()) {
                        return Mono.error(new IllegalStateException("no text in file"));
                    }
                    Mono<List<String>> vectors = properties.getOpenai().configured()
                            ? documents.updateStatus(id, DocumentStatus.EMBEDDING, null, pieces.size())
                            .then(openAi.embed(pieces)
                                    .map(list -> list.stream().map(ChunkRepository::toVectorLiteral).toList())
                                    .onErrorResume(e -> {
                                        log.warn("embeddings failed, saving text chunks only: {}", e.getMessage());
                                        return Mono.just(java.util.Collections.nCopies(pieces.size(), (String) null));
                                    }))
                            : Mono.just(java.util.Collections.nCopies(pieces.size(), (String) null));

                    return vectors.flatMap(embs -> chunks.deleteByDocumentId(id)
                            .thenMany(Flux.range(0, pieces.size())
                                    .concatMap(i -> chunks.insert(
                                            UUID.randomUUID(),
                                            id,
                                            i,
                                            pieces.get(i),
                                            embs.get(i)
                                    )))
                            .then(documents.updateStatus(id, DocumentStatus.READY, null, pieces.size()))
                            .thenReturn(pieces.size()));
                });
    }
}
