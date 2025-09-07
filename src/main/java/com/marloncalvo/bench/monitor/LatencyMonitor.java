package com.marloncalvo.bench.monitor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.marloncalvo.bench.common.BrokerEvent;
import com.marloncalvo.bench.common.KafkaConfig;
import io.prometheus.metrics.core.metrics.Histogram;
import io.prometheus.metrics.model.snapshots.Unit;
import io.prometheus.metrics.exporter.httpserver.HTTPServer;
import io.prometheus.metrics.instrumentation.jvm.JvmMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LatencyMonitor {
    private static final Logger logger = LoggerFactory.getLogger(LatencyMonitor.class);
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final String OUTPUT_TOPIC = System.getenv().getOrDefault("OUTPUT_TOPIC", "output-topic");
    private static final Histogram latency = Histogram.builder()
            .name("message_latency_seconds")
            .help("Message latency from input to output topic.")
            .unit(Unit.SECONDS)
            .classicExponentialUpperBounds(0.00001, 1.2, 120)
            .register();

    public static void main(String[] args) {
        try {
            JvmMetrics.builder().register(); 
            HTTPServer.builder().port(8081).buildAndStart();

            try (var consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<String, String>(KafkaConfig.getConsumerProperties(KafkaConfig.getConsumerGroupId("latency-monitor-group")))) {
                consumer.subscribe(java.util.Collections.singletonList(OUTPUT_TOPIC));

                while (true) {
                    var records = consumer.poll(java.time.Duration.ofMillis(100));
                    for (var record : records) {
                        try {
                            var event = mapper.readValue(record.value(), BrokerEvent.class);
                            long delta = record.timestamp() - event.getBrokerConsumeEventTimestamp();
                            double deltaSeconds = delta / 1000.0;
                            latency.observe(deltaSeconds);
                        } catch (Exception e) {
                            logger.error("Error processing record", e);
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Error in latency monitor", e);
        }
    }
}
