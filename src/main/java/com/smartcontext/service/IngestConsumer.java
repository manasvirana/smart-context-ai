package com.smartcontext.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.smartcontext.dto.DocumentUploadedEvent;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.util.retry.Retry;

import java.time.Duration;

@Component
public class IngestConsumer {

    private static final Logger log = LoggerFactory.getLogger(IngestConsumer.class);

    private final ReceiverOptions<String, String> receiverOptions;
    private final IngestService ingestService;
    private final ObjectMapper objectMapper;
    private Disposable subscription;

    public IngestConsumer(
            ReceiverOptions<String, String> receiverOptions,
            IngestService ingestService,
            ObjectMapper objectMapper
    ) {
        this.receiverOptions = receiverOptions;
        this.ingestService = ingestService;
        this.objectMapper = objectMapper;
    }

    @PostConstruct
    public void start() {
        subscription = Flux.defer(() -> KafkaReceiver.create(receiverOptions).receive())
                .concatMap(this::process)
                .doOnError(e -> log.error("kafka consumer stream failed, restarting", e))
                .retryWhen(Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(1))
                        .maxBackoff(Duration.ofSeconds(30))
                        .transientErrors(true))
                .subscribe();
        log.info("kafka ingest consumer started");
    }

    Mono<Void> process(ReceiverRecord<String, String> record) {
        return Mono.fromCallable(() -> objectMapper.readValue(record.value(), DocumentUploadedEvent.class))
                .flatMap(event -> ingestService.handle(event)
                        .retryWhen(Retry.backoff(5, Duration.ofSeconds(1)).maxBackoff(Duration.ofSeconds(10))))
                .doOnError(e -> log.error("kafka record failed for document {}", record.key(), e))
                .onErrorResume(e -> Mono.empty())
                .then(Mono.defer(() -> record.receiverOffset().commit()));
    }

    @PreDestroy
    public void stop() {
        if (subscription != null) {
            subscription.dispose();
        }
    }
}
