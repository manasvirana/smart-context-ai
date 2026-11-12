package com.smartcontext.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.smartcontext.config.AppProperties;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class OpenAiService {

    private static final int EMBEDDING_BATCH_SIZE = 100;

    private final WebClient webClient;
    private final AppProperties properties;
    private final ObjectMapper objectMapper;
    private final EmbeddingCache embeddingCache;
    private final AppMetrics metrics;

    public OpenAiService(
            WebClient openAiWebClient,
            AppProperties properties,
            ObjectMapper objectMapper,
            EmbeddingCache embeddingCache,
            AppMetrics metrics
    ) {
        this.webClient = openAiWebClient;
        this.properties = properties;
        this.objectMapper = objectMapper;
        this.embeddingCache = embeddingCache;
        this.metrics = metrics;
    }

    public Mono<List<List<Double>>> embed(List<String> inputs) {
        if (inputs.isEmpty()) {
            return Mono.just(List.of());
        }
        String model = properties.getOpenai().getEmbeddingModel();
        return Flux.range(0, inputs.size())
                .concatMap(i -> embeddingCache.get(model, inputs.get(i))
                        .map(v -> new CachedVector(i, v))
                        .defaultIfEmpty(new CachedVector(i, null)))
                .collectList()
                .flatMap(cached -> {
                    List<Integer> missIdx = new ArrayList<>();
                    List<String> missText = new ArrayList<>();
                    for (CachedVector slot : cached) {
                        if (slot.vector == null) {
                            missIdx.add(slot.index);
                            missText.add(inputs.get(slot.index));
                            metrics.cacheMiss("embedding");
                        }
                    }
                    if (missText.isEmpty()) {
                        return Mono.just(cached.stream().map(s -> s.vector).toList());
                    }
                    return embedInBatches(missText)
                            .flatMap(fresh -> {
                                List<List<Double>> ordered = new ArrayList<>(inputs.size());
                                for (int i = 0; i < inputs.size(); i++) {
                                    ordered.add(null);
                                }
                                for (CachedVector slot : cached) {
                                    if (slot.vector != null) {
                                        ordered.set(slot.index, slot.vector);
                                    }
                                }
                                List<Mono<Void>> stores = new ArrayList<>();
                                for (int i = 0; i < missIdx.size(); i++) {
                                    int idx = missIdx.get(i);
                                    List<Double> vector = fresh.get(i);
                                    ordered.set(idx, vector);
                                    stores.add(embeddingCache.put(model, inputs.get(idx), vector));
                                }
                                return Flux.concat(stores).then(Mono.just(ordered));
                            });
                });
    }

    private Mono<List<List<Double>>> embedInBatches(List<String> inputs) {
        return Flux.fromIterable(partition(inputs, EMBEDDING_BATCH_SIZE))
                .concatMap(this::embedRemote)
                .collectList()
                .map(batches -> batches.stream().flatMap(List::stream).toList());
    }

    private static List<List<String>> partition(List<String> items, int size) {
        List<List<String>> batches = new ArrayList<>();
        for (int i = 0; i < items.size(); i += size) {
            batches.add(items.subList(i, Math.min(i + size, items.size())));
        }
        return batches;
    }

    private Mono<List<List<Double>>> embedRemote(List<String> inputs) {
        Map<String, Object> body = new HashMap<>();
        body.put("model", properties.getOpenai().getEmbeddingModel());
        body.put("input", inputs);

        return webClient.post()
                .uri("/v1/embeddings")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(body)
                .retrieve()
                .bodyToMono(JsonNode.class)
                .timeout(Duration.ofSeconds(25))
                .retryWhen(retry429())
                .map(root -> {
                    List<List<Double>> out = new ArrayList<>();
                    for (JsonNode item : root.get("data")) {
                        List<Double> vector = new ArrayList<>();
                        for (JsonNode n : item.get("embedding")) {
                            vector.add(n.asDouble());
                        }
                        out.add(vector);
                    }
                    metrics.openaiCall("embed", "success");
                    return out;
                })
                .doOnError(e -> metrics.openaiCall("embed", statusLabel(e)));
    }

    public Flux<String> streamChat(String systemPrompt, String userPrompt) {
        Map<String, Object> body = new HashMap<>();
        body.put("model", properties.getOpenai().getChatModel());
        body.put("stream", true);
        body.put("messages", List.of(
                Map.of("role", "system", "content", systemPrompt),
                Map.of("role", "user", "content", userPrompt)
        ));

        return webClient.post()
                .uri("/v1/chat/completions")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.TEXT_EVENT_STREAM)
                .bodyValue(body)
                .retrieve()
                .bodyToFlux(String.class)
                .timeout(Duration.ofSeconds(25))
                .retryWhen(retry429())
                .concatMap(this::delta)
                .doOnComplete(() -> metrics.openaiCall("chat", "success"))
                .doOnError(e -> metrics.openaiCall("chat", statusLabel(e)));
    }

    private Retry retry429() {
        return Retry.backoff(2, Duration.ofSeconds(1))
                .maxBackoff(Duration.ofSeconds(8))
                .filter(OpenAiService::is429)
                .transientErrors(true);
    }

    private static boolean is429(Throwable e) {
        if (e instanceof WebClientResponseException w) {
            HttpStatusCode status = w.getStatusCode();
            return status.value() == 429;
        }
        return false;
    }

    private static String statusLabel(Throwable e) {
        if (e instanceof WebClientResponseException w && w.getStatusCode().value() == 429) {
            return "429";
        }
        return "error";
    }

    private Mono<String> delta(String payload) {
        if (payload == null || payload.isBlank() || "[DONE]".equals(payload.trim())) {
            return Mono.empty();
        }
        try {
            JsonNode content = objectMapper.readTree(payload).path("choices").path(0).path("delta").path("content");
            if (content.isMissingNode() || content.asText().isEmpty()) {
                return Mono.empty();
            }
            return Mono.just(content.asText());
        } catch (Exception e) {
            return Mono.empty();
        }
    }

    private record CachedVector(int index, List<Double> vector) {
    }
}
