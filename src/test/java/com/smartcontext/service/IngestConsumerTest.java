package com.smartcontext.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.smartcontext.dto.DocumentUploadedEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverOffset;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class IngestConsumerTest {

    private IngestService ingestService;
    private ReceiverRecord<String, String> record;
    private ReceiverOffset offset;
    private IngestConsumer consumer;
    private final AtomicInteger handleCalls = new AtomicInteger();
    private final AtomicInteger callsAtCommit = new AtomicInteger(-1);

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() throws Exception {
        ingestService = mock(IngestService.class);
        record = mock(ReceiverRecord.class);
        offset = mock(ReceiverOffset.class);
        ObjectMapper mapper = new ObjectMapper();
        DocumentUploadedEvent event = new DocumentUploadedEvent(UUID.randomUUID(), "path", "a.txt", "text/plain");
        when(record.value()).thenReturn(mapper.writeValueAsString(event));
        when(record.key()).thenReturn(event.getDocumentId().toString());
        when(record.receiverOffset()).thenReturn(offset);
        when(offset.commit()).thenAnswer(invocation -> {
            callsAtCommit.set(handleCalls.get());
            return Mono.empty();
        });
        consumer = new IngestConsumer(mock(ReceiverOptions.class), ingestService, mapper);
    }

    @Test
    void retriesIngestThenCommitsAfterItSucceeds() {
        handleFailsTimes(2);

        StepVerifier.withVirtualTime(() -> consumer.process(record))
                .thenAwait(Duration.ofSeconds(30))
                .verifyComplete();

        assertEquals(3, handleCalls.get());
        assertEquals(3, callsAtCommit.get());
        verify(offset, times(1)).commit();
    }

    @Test
    void givesUpAfterRetriesAndStillCommits() {
        handleFailsTimes(Integer.MAX_VALUE);

        StepVerifier.withVirtualTime(() -> consumer.process(record))
                .thenAwait(Duration.ofMinutes(2))
                .verifyComplete();

        assertEquals(6, handleCalls.get());
        verify(offset, times(1)).commit();
    }

    @Test
    void skipsAndCommitsInvalidJson() {
        when(record.value()).thenReturn("not json");

        StepVerifier.create(consumer.process(record)).verifyComplete();

        verify(ingestService, never()).handle(any());
        verify(offset, times(1)).commit();
    }

    @Test
    void propagatesCommitFailureSoStreamCanRestart() {
        handleFailsTimes(0);
        when(offset.commit()).thenReturn(Mono.error(new IllegalStateException("commit failed")));

        StepVerifier.create(consumer.process(record))
                .expectErrorMessage("commit failed")
                .verify();
    }

    private void handleFailsTimes(int failures) {
        when(ingestService.handle(any())).thenReturn(Mono.defer(() -> {
            if (handleCalls.incrementAndGet() <= failures) {
                return Mono.<Void>error(new IllegalStateException("db down"));
            }
            return Mono.<Void>empty();
        }));
    }
}
