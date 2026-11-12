package com.smartcontext.service;

import com.smartcontext.config.AppProperties;
import com.smartcontext.dto.DocumentUploadedEvent;
import com.smartcontext.model.DocumentStatus;
import com.smartcontext.repository.ChunkRepository;
import com.smartcontext.repository.DocumentRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class IngestServiceTest {

    private DocumentRepository documents;
    private ChunkRepository chunks;
    private OpenAiService openAi;
    private AppProperties properties;
    private IngestService service;
    private UUID id;

    @BeforeEach
    void setUp() {
        documents = mock(DocumentRepository.class);
        chunks = mock(ChunkRepository.class);
        DocumentParserService parser = mock(DocumentParserService.class);
        openAi = mock(OpenAiService.class);
        properties = new AppProperties();
        properties.getOpenai().setApiKey("sk-test");
        id = UUID.randomUUID();
        service = new IngestService(documents, chunks, parser, openAi, properties, new AppMetrics(new SimpleMeterRegistry()));

        when(documents.claimForProcessing(id, false)).thenReturn(Mono.just(true));
        when(documents.updateStatus(any(), any(), any(), any())).thenReturn(Mono.empty());
        when(parser.extractText(any(), any(), any())).thenReturn(Mono.just("text"));
        when(parser.chunk(any(), anyInt(), anyInt())).thenReturn(List.of("one", "two"));
        when(chunks.deleteByDocumentId(id)).thenReturn(Mono.empty());
        when(chunks.insert(any(), any(), anyInt(), any(), any())).thenReturn(Mono.empty());
    }

    @Test
    void marksReadyWithWarningWhenEmbeddingFails() {
        when(openAi.embed(any())).thenReturn(Mono.error(new IllegalStateException("openai down")));

        StepVerifier.create(service.handle(event())).verifyComplete();

        verify(documents).updateStatus(
                eq(id),
                eq(DocumentStatus.READY),
                argThat((String warning) -> warning != null && warning.contains("keyword search only")),
                eq(2)
        );
        verify(chunks, times(2)).insert(any(), eq(id), anyInt(), any(), isNull());
    }

    @Test
    void marksReadyWithoutWarningWhenEmbeddingSucceeds() {
        when(openAi.embed(any())).thenReturn(Mono.just(List.of(List.of(0.1), List.of(0.2))));

        StepVerifier.create(service.handle(event())).verifyComplete();

        verify(documents).updateStatus(eq(id), eq(DocumentStatus.READY), isNull(), eq(2));
    }

    @Test
    void marksReadyWithoutWarningWhenOpenAiIsNotConfigured() {
        properties.getOpenai().setApiKey("");

        StepVerifier.create(service.handle(event())).verifyComplete();

        verify(openAi, never()).embed(any());
        verify(documents).updateStatus(eq(id), eq(DocumentStatus.READY), isNull(), eq(2));
    }

    private DocumentUploadedEvent event() {
        return new DocumentUploadedEvent(id, "path", "a.txt", "text/plain");
    }
}
