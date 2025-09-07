package com.marloncalvo.bench.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;

public class AppConfig {
    @JsonProperty("concurrentEventProcessing")
    private int concurrentEventProcessing;

    @JsonProperty("updateTime")
    private Instant updateTime;

    @JsonProperty("batchSize")
    private int batchSize;

    @JsonProperty("loadGeneratorRate")
    private double loadGeneratorRate;

    public double getLoadGeneratorRate() {
		return loadGeneratorRate;
	}

	public void setLoadGeneratorRate(double loadGeneratorRate) {
		this.loadGeneratorRate = loadGeneratorRate;
	}

	public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public int getConcurrentEventProcessing() {
        return concurrentEventProcessing;
    }

    public void setConcurrentEventProcessing(int concurrentEventProcessing) {
        this.concurrentEventProcessing = concurrentEventProcessing;
    }

    public Instant getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Instant updateTime) {
        this.updateTime = updateTime;
    }
}
