package com.marloncalvo.bench.config;

import java.util.concurrent.Semaphore;

import com.google.common.util.concurrent.RateLimiter;

/**
 * An immutable holder for pipeline configuration.
 * An instance of this class is created each time the configuration is reloaded.
 */
public class PipelineConfig {
    private final int concurrentEventProcessing;
    private final int batchSize;
    private final RateLimiter loadGeneratorRateLimiter;

	private final Semaphore webServiceSemaphore;

    /**
     * Initializes a new configuration holder from a raw AppConfig object.
     * This is where derived configuration objects, like the Semaphore, are created.
     * @param appConfig The raw configuration object parsed from the JSON endpoint.
     */
    public PipelineConfig(AppConfig appConfig) {
        // Use a default value if appConfig is null during initial startup
        int permits = (appConfig != null) ? appConfig.getConcurrentEventProcessing() : 20;
        if (permits <= 0) {
            permits = 20; // Safeguard against invalid config values
        }
        this.concurrentEventProcessing = permits;
        this.webServiceSemaphore = new Semaphore(this.concurrentEventProcessing);
        this.batchSize = (appConfig != null) ? appConfig.getBatchSize() : Integer.MAX_VALUE;
        this.loadGeneratorRateLimiter = RateLimiter.create((appConfig != null) ? appConfig.getLoadGeneratorRate() : 500.0);
    }

    public RateLimiter getLoadGeneratorRateLimiter() {
		return loadGeneratorRateLimiter;
	}

    public Semaphore getWebServiceSemaphore() {
        return webServiceSemaphore;
    }

    public int getConcurrentEventProcessing() {
        return concurrentEventProcessing;
    }

    public int getBatchSize() {
        return batchSize;
    }
}
