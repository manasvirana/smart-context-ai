package com.smartcontext.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.smartcontext.config.AppProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;

@Service
public class EmbeddingCache {

    private static final Logger log = LoggerFactory.getLogger(EmbeddingCache.class);
    private static final TypeReference<List<Double>> VECTOR = new TypeReference<>() {
    };

    private final ReactiveStringRedisTemplate redis;
    private final ObjectMapper objectMapper;
    private final AppProperties properties;
    private final AppMetrics metrics;

    public EmbeddingCache(
            ReactiveStringRedisTemplate redis,
            ObjectMapper objectMapper,
            AppProperties properties,
            AppMetrics metrics
    ) {
        this.redis = redis;
        this.objectMapper = objectMapper;
        this.properties = properties;
        this.metrics = metrics;
    }

    public Mono<List<Double>> get(String model, String text) {
        return redis.opsForValue().get(key(model, text))
                .flatMap(json -> Mono.fromCallable(() -> objectMapper.readValue(json, VECTOR)))
                .doOnNext(v -> metrics.cacheHit("embedding"))
                .onErrorResume(e -> {
                    log.warn("embedding cache read failed: {}", e.getMessage());
                    return Mono.empty();
                });
    }

    public Mono<Void> put(String model, String text, List<Double> vector) {
        return Mono.fromCallable(() -> objectMapper.writeValueAsString(vector))
                .flatMap(json -> redis.opsForValue().set(key(model, text), json, properties.getCache().getEmbeddingTtl()))
                .then()
                .onErrorResume(e -> {
                    log.warn("embedding cache write failed: {}", e.getMessage());
                    return Mono.empty();
                });
    }

    private String key(String model, String text) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest((model + "\n" + text).getBytes(StandardCharsets.UTF_8));
            return "sc:emb:" + HexFormat.of().formatHex(hash);
        } catch (Exception e) {
            throw new IllegalStateException("cannot hash embedding cache key", e);
        }
    }
}
