package com.marloncalvo.bench.pipelines;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.marloncalvo.bench.common.BrokerEvent;
import com.marloncalvo.bench.common.KafkaConfig;
import com.marloncalvo.bench.common.Service;

import io.prometheus.metrics.core.metrics.Histogram;
import io.prometheus.metrics.exporter.httpserver.HTTPServer;
import io.prometheus.metrics.instrumentation.jvm.JvmMetrics;
import io.prometheus.metrics.model.snapshots.Unit;

public abstract class BasePipeline {
    private static final ObjectMapper mapper = new ObjectMapper();
    protected static final Logger logger = LoggerFactory.getLogger(
        BasePipeline.class
    );

    private static final Histogram processingTime = Histogram.builder()
        .name("pipeline_record_processing_time_seconds")
        .help("Processing time per record in pipeline")
        .unit(Unit.SECONDS)
        .classicExponentialUpperBounds(0.00001, 1.2, 120)
        .register();

    private static final Histogram serviceCallTime = Histogram.builder()
        .name("service_endpoint_call_time_seconds")
        .help("Time taken for service endpoint calls")
        .unit(Unit.SECONDS)
        .classicExponentialUpperBounds(0.00001, 1.2, 120)
        .register();

    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private final Service service;

    public BasePipeline() {
        this.service = new Service();
        JvmMetrics.builder().register(); 
        try {
            HTTPServer.builder().port(8082).buildAndStart();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected abstract void Run();

    protected void serviceEndpointCall() {
        long startTime = System.currentTimeMillis();
        this.service.get();
        long duration = System.currentTimeMillis() - startTime;
        serviceCallTime.observe(duration / 1000.0);
    }

    protected void observeProcessingTime(long durationMillis) {
        processingTime.observe(durationMillis / 1_000.0);
    }

    protected String getInputTopic() {
        return System.getenv().getOrDefault("INPUT_TOPIC", "input-topic");
    }

    protected String getOutputTopic() {
        return System.getenv().getOrDefault("OUTPUT_TOPIC", "output-topic");
    }

    protected String getConsumerGroupId() {
        return KafkaConfig.getConsumerGroupId("pipeline-bench");
    }

    protected String getKafkaEventSerialized(long recordTimestamp) throws JsonProcessingException {
        BrokerEvent event = new BrokerEvent(recordTimestamp);
        String json = mapper.writeValueAsString(event);
        return json;
    }

    protected ExecutorService getExecutorService() {
        return this.executor;
    }
}