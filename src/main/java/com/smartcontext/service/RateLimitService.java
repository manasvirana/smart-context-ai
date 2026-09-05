package com.smartcontext.service;

import com.smartcontext.config.AppProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;

@Service
public class RateLimitService {

    private static final Logger log = LoggerFactory.getLogger(RateLimitService.class);

    private final ReactiveStringRedisTemplate redis;
    private final AppProperties properties;
    private final AppMetrics metrics;

    public RateLimitService(ReactiveStringRedisTemplate redis, AppProperties properties, AppMetrics metrics) {
        this.redis = redis;
        this.properties = properties;
        this.metrics = metrics;
    }

    public Mono<Boolean> allow(String clientId) {
        if (!properties.getRateLimit().isEnabled()) {
            return Mono.just(true);
        }
        int limit = Math.max(1, properties.getRateLimit().getRequestsPerMinute());
        long window = Instant.now().getEpochSecond() / 60;
        String key = "sc:rl:" + clientId + ":" + window;
        return redis.opsForValue().increment(key)
                .flatMap(count -> {
                    Mono<Boolean> expire = count == 1
                            ? redis.expire(key, Duration.ofMinutes(2))
                            : Mono.just(true);
                    boolean ok = count <= limit;
                    if (!ok) {
                        metrics.rateLimited();
                    }
                    return expire.thenReturn(ok);
                })
                .onErrorResume(e -> {
                    log.warn("rate limit redis failed, allowing request: {}", e.getMessage());
                    return Mono.just(true);
                });
    }
}
