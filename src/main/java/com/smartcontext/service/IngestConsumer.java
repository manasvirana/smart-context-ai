package com.smartcontext.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.smartcontext.dto.DocumentUploadedEvent;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

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
        subscription = KafkaReceiver.create(receiverOptions)
                .receive()
                .concatMap(record -> Mono.fromCallable(() -> objectMapper.readValue(record.value(), DocumentUploadedEvent.class))
                        .flatMap(ingestService::handle)
                        .then(record.receiverOffset().commit())
                        .doOnError(e -> log.error("kafka record failed", e))
                        .onErrorResume(e -> record.receiverOffset().commit()))
                .subscribe();
        log.info("kafka ingest consumer started");
    }

    @PreDestroy
    public void stop() {
        if (subscription != null) {
            subscription.dispose();
        }
    }
}
