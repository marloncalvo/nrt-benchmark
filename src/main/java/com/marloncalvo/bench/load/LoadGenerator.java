package com.marloncalvo.bench.load;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.marloncalvo.bench.common.BrokerEvent;
import com.marloncalvo.bench.common.KafkaConfig;
import com.marloncalvo.bench.config.ConfigService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoadGenerator {
    private static final Logger logger = LoggerFactory.getLogger(LoadGenerator.class);
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final String INPUT_TOPIC = System.getenv().getOrDefault("INPUT_TOPIC", "input-topic");

    public static void main(String[] args) {
        var configService = new ConfigService();
        try (var producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(KafkaConfig.getProducerProperties())) {
            while (true) {
                configService.getInstance().getLoadGeneratorRateLimiter().acquire();

                var timestamp = System.currentTimeMillis();
                BrokerEvent event = new BrokerEvent(timestamp);
                String json = mapper.writeValueAsString(event);
                producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>(INPUT_TOPIC, null, json));
            }
        } catch (Exception e) {
            logger.error("Error in load generator", e);
        }
    }
}
