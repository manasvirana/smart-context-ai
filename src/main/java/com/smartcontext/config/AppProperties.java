package com.smartcontext.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

@ConfigurationProperties(prefix = "app")
public class AppProperties {

    private String uploadDir = "./data/uploads";
    private final Kafka kafka = new Kafka();
    private final OpenAi openai = new OpenAi();
    private final Security security = new Security();
    private final RateLimit rateLimit = new RateLimit();
    private final Cache cache = new Cache();

    public String getUploadDir() {
        return uploadDir;
    }

    public void setUploadDir(String uploadDir) {
        this.uploadDir = uploadDir;
    }

    public Kafka getKafka() {
        return kafka;
    }

    public OpenAi getOpenai() {
        return openai;
    }

    public Security getSecurity() {
        return security;
    }

    public RateLimit getRateLimit() {
        return rateLimit;
    }

    public Cache getCache() {
        return cache;
    }

    public static class Kafka {
        private String bootstrapServers = "localhost:9092";
        private String topic = "document.uploaded";
        private String groupId = "smart-context-ingest";

        public String getBootstrapServers() {
            return bootstrapServers;
        }

        public void setBootstrapServers(String bootstrapServers) {
            this.bootstrapServers = bootstrapServers;
        }

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public String getGroupId() {
            return groupId;
        }

        public void setGroupId(String groupId) {
            this.groupId = groupId;
        }
    }

    public static class OpenAi {
        private String apiKey = "";
        private String baseUrl = "https://api.openai.com";
        private String embeddingModel = "text-embedding-3-small";
        private String chatModel = "gpt-4o-mini";

        public String getApiKey() {
            return apiKey;
        }

        public void setApiKey(String apiKey) {
            this.apiKey = apiKey;
        }

        public String getBaseUrl() {
            return baseUrl;
        }

        public void setBaseUrl(String baseUrl) {
            this.baseUrl = baseUrl;
        }

        public String getEmbeddingModel() {
            return embeddingModel;
        }

        public void setEmbeddingModel(String embeddingModel) {
            this.embeddingModel = embeddingModel;
        }

        public String getChatModel() {
            return chatModel;
        }

        public void setChatModel(String chatModel) {
            this.chatModel = chatModel;
        }

        public boolean configured() {
            return apiKey != null && !apiKey.isBlank();
        }
    }

    public static class Security {
        private String apiKeys = "";

        public String getApiKeys() {
            return apiKeys;
        }

        public void setApiKeys(String apiKeys) {
            this.apiKeys = apiKeys;
        }

        public List<String> configuredKeys() {
            if (apiKeys == null || apiKeys.isBlank()) {
                return List.of();
            }
            return Arrays.stream(apiKeys.split(","))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .toList();
        }

        public boolean requiresApiKey() {
            return !configuredKeys().isEmpty();
        }
    }

    public static class RateLimit {
        private boolean enabled = true;
        private int requestsPerMinute = 60;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public int getRequestsPerMinute() {
            return requestsPerMinute;
        }

        public void setRequestsPerMinute(int requestsPerMinute) {
            this.requestsPerMinute = requestsPerMinute;
        }
    }

    public static class Cache {
        private Duration embeddingTtl = Duration.ofHours(24);

        public Duration getEmbeddingTtl() {
            return embeddingTtl;
        }

        public void setEmbeddingTtl(Duration embeddingTtl) {
            this.embeddingTtl = embeddingTtl;
        }
    }
}
