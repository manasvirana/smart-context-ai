package com.smartcontext.filter;

import com.smartcontext.config.AppProperties;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.List;

@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class ApiKeyWebFilter implements WebFilter {

    static final String HEADER = "X-API-Key";
    static final String ATTR_CLIENT = "clientId";

    private final AppProperties properties;

    public ApiKeyWebFilter(AppProperties properties) {
        this.properties = properties;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        String path = exchange.getRequest().getPath().value();
        if (isPublic(path)) {
            exchange.getAttributes().put(ATTR_CLIENT, "anonymous");
            return chain.filter(exchange);
        }
        List<String> keys = properties.getSecurity().configuredKeys();
        if (keys.isEmpty()) {
            exchange.getAttributes().put(ATTR_CLIENT, "anonymous");
            return chain.filter(exchange);
        }
        String provided = exchange.getRequest().getHeaders().getFirst(HEADER);
        if (provided == null || !keys.contains(provided)) {
            return unauthorized(exchange);
        }
        exchange.getAttributes().put(ATTR_CLIENT, provided);
        return chain.filter(exchange);
    }

    static boolean isPublic(String path) {
        return path.equals("/v1/health")
                || path.startsWith("/actuator");
    }

    private Mono<Void> unauthorized(ServerWebExchange exchange) {
        exchange.getResponse().setStatusCode(HttpStatus.UNAUTHORIZED);
        exchange.getResponse().getHeaders().setContentType(MediaType.APPLICATION_JSON);
        byte[] body = "{\"error\":\"missing or invalid X-API-Key\"}".getBytes(StandardCharsets.UTF_8);
        return exchange.getResponse().writeWith(Mono.just(exchange.getResponse().bufferFactory().wrap(body)));
    }
}
