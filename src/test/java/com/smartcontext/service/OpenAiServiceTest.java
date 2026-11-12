package com.smartcontext.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.smartcontext.config.AppProperties;
import com.sun.net.httpserver.HttpServer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class OpenAiServiceTest {

    private HttpServer server;
    private EmbeddingCache cache;
    private OpenAiService service;
    private final List<Integer> batchSizes = new CopyOnWriteArrayList<>();

    @BeforeEach
    void setUp() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/embeddings", exchange -> {
            JsonNode input = mapper.readTree(exchange.getRequestBody()).get("input");
            batchSizes.add(input.size());
            ArrayNode data = mapper.createArrayNode();
            for (JsonNode text : input) {
                data.addObject().putArray("embedding").add(Double.parseDouble(text.asText().substring(1)));
            }
            byte[] body = mapper.writeValueAsBytes(mapper.createObjectNode().set("data", data));
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();

        cache = mock(EmbeddingCache.class);
        when(cache.get(any(), any())).thenReturn(Mono.empty());
        when(cache.put(any(), any(), any())).thenReturn(Mono.empty());
        service = new OpenAiService(
                WebClient.create("http://127.0.0.1:" + server.getAddress().getPort()),
                new AppProperties(),
                mapper,
                cache,
                new AppMetrics(new SimpleMeterRegistry())
        );
    }

    @AfterEach
    void tearDown() {
        server.stop(0);
    }

    @Test
    void splitsLargeInputIntoBatchesAndKeepsOrder() {
        List<String> inputs = IntStream.range(0, 250).mapToObj(i -> "t" + i).toList();

        StepVerifier.create(service.embed(inputs))
                .assertNext(vectors -> {
                    assertEquals(250, vectors.size());
                    for (int i = 0; i < 250; i++) {
                        assertEquals((double) i, vectors.get(i).get(0));
                    }
                })
                .verifyComplete();

        assertEquals(List.of(100, 100, 50), batchSizes);
    }

    @Test
    void usesOneRequestForSmallInput() {
        StepVerifier.create(service.embed(List.of("t0", "t1", "t2")))
                .assertNext(vectors -> assertEquals(3, vectors.size()))
                .verifyComplete();

        assertEquals(List.of(3), batchSizes);
    }

    @Test
    void requestsOnlyCacheMissesAndKeepsOrder() {
        when(cache.get(any(), eq("t1"))).thenReturn(Mono.just(List.of(-1.0)));

        StepVerifier.create(service.embed(List.of("t0", "t1", "t2")))
                .assertNext(vectors -> {
                    assertEquals(List.of(0.0), vectors.get(0));
                    assertEquals(List.of(-1.0), vectors.get(1));
                    assertEquals(List.of(2.0), vectors.get(2));
                })
                .verifyComplete();

        assertEquals(List.of(2), batchSizes);
    }
}
