package com.marloncalvo.bench.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A background service that polls a configuration endpoint and manages the lifecycle
 * of immutable PipelineConfig instances.
 */
public class ConfigService {
    private static final Logger logger = LoggerFactory.getLogger(ConfigService.class);
    private static final String CONFIG_ENDPOINT = System.getenv().getOrDefault("CONFIG_ENDPOINT", "http://pipeline-config:8060/config");
    private static final long POLLING_INTERVAL_MS = 5000;

    private static final AtomicReference<PipelineConfig> currentConfig = new AtomicReference<>(new PipelineConfig(null));
    private static final AtomicReference<Instant> lastUpdateTime = new AtomicReference<>();
    private static final ObjectMapper mapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private static final HttpClient httpClient = HttpClient.newHttpClient();

    // Static initializer to start the polling service
    public ConfigService() {
        loadConfigFromHttp();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "config-poller");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleAtFixedRate(ConfigService::loadConfigFromHttp, 0, POLLING_INTERVAL_MS, TimeUnit.MILLISECONDS);
        logger.info("Started polling configuration endpoint {} every {}ms", CONFIG_ENDPOINT, POLLING_INTERVAL_MS);
    }

    /**
     * Returns the current, active pipeline configuration.
     * This is the single public access point to the configuration system.
     * @return The latest immutable PipelineConfig instance.
     */
    public PipelineConfig getInstance() {
        return currentConfig.get();
    }

    private static void loadConfigFromHttp() {
        try {
            HttpRequest request = HttpRequest.newBuilder().uri(URI.create(CONFIG_ENDPOINT)).build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 200) {
                AppConfig newAppConfig = mapper.readValue(response.body(), AppConfig.class);
                Instant newUpdateTime = newAppConfig.getUpdateTime();
                Instant currentUpdateTime = lastUpdateTime.get();

                if (currentUpdateTime == null || newUpdateTime.isAfter(currentUpdateTime)) {
                    lastUpdateTime.set(newUpdateTime);
                    PipelineConfig newPipelineConfig = new PipelineConfig(newAppConfig);
                    currentConfig.set(newPipelineConfig); // Atomically swap to the new config
                    logger.info("New configuration applied. UpdateTime: {}. Permits: {}", newUpdateTime, newPipelineConfig.getConcurrentEventProcessing());
                }
            } else {
                logger.warn("Failed to fetch configuration. Status code: {}", response.statusCode());
            }
        } catch (Exception e) {
            logger.error("Error loading configuration from HTTP endpoint", e);
        }
    }
}
