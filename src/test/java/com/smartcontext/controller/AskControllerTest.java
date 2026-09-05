package com.smartcontext.controller;

import com.smartcontext.config.AppProperties;
import com.smartcontext.dto.AskRequest;
import com.smartcontext.filter.ApiKeyWebFilter;
import com.smartcontext.service.AskService;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import com.smartcontext.service.RateLimitService;

@WebFluxTest(controllers = {AskController.class, RestExceptionHandler.class})
@Import(ApiKeyWebFilter.class)
@EnableConfigurationProperties(AppProperties.class)
@TestPropertySource(properties = "app.security.api-keys=test-key")
class AskControllerTest {

    @Autowired
    private WebTestClient client;

    @MockitoBean
    private AskService askService;

    @MockitoBean
    private RateLimitService rateLimitService;

    @Test
    void rejectsMissingApiKey() {
        Mockito.when(rateLimitService.allow(Mockito.any())).thenReturn(Mono.just(true));
        client.post().uri("/v1/ask")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{\"query\":\"hello\"}")
                .exchange()
                .expectStatus().isUnauthorized();
    }

    @Test
    void streamsWhenApiKeyValid() {
        Mockito.when(rateLimitService.allow(Mockito.any())).thenReturn(Mono.just(true));
        Mockito.when(askService.ask(Mockito.any(AskRequest.class)))
                .thenReturn(Flux.just(ServerSentEvent.<String>builder().event("done").data("ok").build()));

        client.post().uri("/v1/ask")
                .header("X-API-Key", "test-key")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{\"query\":\"hello\"}")
                .exchange()
                .expectStatus().isOk();
    }
}
