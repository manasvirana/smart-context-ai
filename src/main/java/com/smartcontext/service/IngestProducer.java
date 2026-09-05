package com.smartcontext.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.smartcontext.config.AppProperties;
import com.smartcontext.dto.DocumentUploadedEvent;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

@Service
public class IngestProducer {

    private final KafkaSender<String, String> kafkaSender;
    private final AppProperties properties;
    private final ObjectMapper objectMapper;

    public IngestProducer(KafkaSender<String, String> kafkaSender, AppProperties properties, ObjectMapper objectMapper) {
        this.kafkaSender = kafkaSender;
        this.properties = properties;
        this.objectMapper = objectMapper;
    }

    public Mono<Void> publish(DocumentUploadedEvent event) {
        return Mono.fromCallable(() -> objectMapper.writeValueAsString(event))
                .map(json -> SenderRecord.create(
                        new ProducerRecord<>(
                                properties.getKafka().getTopic(),
                                event.getDocumentId().toString(),
                                json
                        ),
                        event.getDocumentId().toString()
                ))
                .flatMap(record -> kafkaSender.send(Mono.just(record)).then());
    }
}
