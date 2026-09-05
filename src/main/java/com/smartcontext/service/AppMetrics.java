package com.smartcontext.service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

@Component
public class AppMetrics {

    private final MeterRegistry registry;

    public AppMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    public void ingestSuccess() {
        counter("smartcontext.ingest", "result", "success").increment();
    }

    public void ingestFailed() {
        counter("smartcontext.ingest", "result", "failed").increment();
    }

    public void ingestSkipped() {
        counter("smartcontext.ingest", "result", "skipped").increment();
    }

    public void retrieval(String mode) {
        counter("smartcontext.retrieval", "mode", mode).increment();
    }

    public void openaiCall(String operation, String result) {
        counter("smartcontext.openai", "operation", operation, "result", result).increment();
    }

    public void cacheHit(String kind) {
        counter("smartcontext.cache", "kind", kind, "result", "hit").increment();
    }

    public void cacheMiss(String kind) {
        counter("smartcontext.cache", "kind", kind, "result", "miss").increment();
    }

    public void rateLimited() {
        counter("smartcontext.rate_limit", "result", "rejected").increment();
    }

    private Counter counter(String name, String... tags) {
        return Counter.builder(name).tags(tags).register(registry);
    }
}
