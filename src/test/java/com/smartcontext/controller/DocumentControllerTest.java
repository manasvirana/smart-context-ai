package com.smartcontext.controller;

import com.smartcontext.config.AppProperties;
import com.smartcontext.filter.ApiKeyWebFilter;
import com.smartcontext.service.DocumentService;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;
import com.smartcontext.service.RateLimitService;

import java.util.UUID;

@WebFluxTest(controllers = {DocumentController.class, RestExceptionHandler.class})
@Import(ApiKeyWebFilter.class)
@EnableConfigurationProperties(AppProperties.class)
class DocumentControllerTest {

    @Autowired
    private WebTestClient client;

    @MockitoBean
    private DocumentService documentService;

    @MockitoBean
    private RateLimitService rateLimitService;

    @Test
    void statusReturns404WhenMissing() {
        Mockito.when(rateLimitService.allow(Mockito.any())).thenReturn(Mono.just(true));
        UUID id = UUID.fromString("11111111-1111-1111-1111-111111111111");
        Mockito.when(documentService.getStatus(id)).thenReturn(Mono.empty());

        client.get().uri("/v1/documents/{id}", id)
                .exchange()
                .expectStatus().isNotFound();
    }

    @Test
    void deleteReturns204() {
        Mockito.when(rateLimitService.allow(Mockito.any())).thenReturn(Mono.just(true));
        UUID id = UUID.fromString("11111111-1111-1111-1111-111111111111");
        Mockito.when(documentService.delete(id)).thenReturn(Mono.empty());

        client.delete().uri("/v1/documents/{id}", id)
                .exchange()
                .expectStatus().isNoContent();
    }

    @Test
    void reindexMapsNotFound() {
        Mockito.when(rateLimitService.allow(Mockito.any())).thenReturn(Mono.just(true));
        UUID id = UUID.fromString("11111111-1111-1111-1111-111111111111");
        Mockito.when(documentService.reindex(id))
                .thenReturn(Mono.error(new DocumentService.NotFoundException("document not found")));

        client.post().uri("/v1/documents/{id}/reindex", id)
                .contentType(MediaType.APPLICATION_JSON)
                .exchange()
                .expectStatus().isNotFound()
                .expectBody()
                .jsonPath("$.error").isEqualTo("document not found");
    }
}
