package com.smartcontext.service;

import com.smartcontext.config.AppProperties;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.ReactiveValueOperations;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RateLimitServiceTest {

    private ReactiveStringRedisTemplate redis;
    private ReactiveValueOperations<String, String> values;
    private RateLimitService service;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        redis = mock(ReactiveStringRedisTemplate.class);
        values = mock(ReactiveValueOperations.class);
        when(redis.opsForValue()).thenReturn(values);
        AppProperties properties = new AppProperties();
        properties.getRateLimit().setEnabled(true);
        properties.getRateLimit().setRequestsPerMinute(2);
        service = new RateLimitService(redis, properties, new AppMetrics(new SimpleMeterRegistry()));
    }

    @Test
    void allowsUnderLimit() {
        when(values.increment(anyString())).thenReturn(Mono.just(1L));
        when(redis.expire(anyString(), any(Duration.class))).thenReturn(Mono.just(true));
        StepVerifier.create(service.allow("key-1")).expectNext(true).verifyComplete();
    }

    @Test
    void rejectsOverLimit() {
        when(values.increment(anyString())).thenReturn(Mono.just(3L));
        StepVerifier.create(service.allow("key-1")).expectNext(false).verifyComplete();
    }

    @Test
    void failOpenWhenRedisDown() {
        when(values.increment(anyString())).thenReturn(Mono.error(new IllegalStateException("redis down")));
        StepVerifier.create(service.allow("key-1")).expectNext(true).verifyComplete();
    }
}
