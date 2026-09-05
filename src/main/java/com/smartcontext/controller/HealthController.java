package com.smartcontext.controller;

import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.util.LinkedHashMap;
import java.util.Map;

@RestController
public class HealthController {

    private final DatabaseClient db;
    private final ReactiveStringRedisTemplate redis;

    public HealthController(DatabaseClient db, ReactiveStringRedisTemplate redis) {
        this.db = db;
        this.redis = redis;
    }

    @GetMapping("/v1/health")
    public Mono<Map<String, String>> health() {
        Mono<String> postgres = db.sql("SELECT 1 AS n")
                .map((row, meta) -> row.get("n", Integer.class))
                .one()
                .map(n -> Integer.valueOf(1).equals(n) ? "UP" : "DOWN")
                .onErrorReturn("DOWN");

        Mono<String> redisStatus = redis.hasKey("__health__")
                .map(ignored -> "UP")
                .onErrorReturn("DOWN");

        return Mono.zip(postgres, redisStatus)
                .map(tuple -> {
                    Map<String, String> body = new LinkedHashMap<>();
                    body.put("app", "UP");
                    body.put("postgres", tuple.getT1());
                    body.put("redis", tuple.getT2());
                    return body;
                });
    }
}
