package com.marloncalvo.bench.common;

import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Utility class for Kafka configuration properties.
 */
public class KafkaConfig {
    private static final String BOOTSTRAP_SERVERS = System.getenv().getOrDefault("KAFKA_ENDPOINT", "kafka:9092");
    public static final String INPUT_TOPIC = System.getenv().getOrDefault("INPUT_TOPIC", "input-topic");

    /**
     * Returns common Kafka properties (bootstrap servers).
     */
    public static Properties getCommonProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        return props;
    }

    /**
     * Returns properties configured for a Kafka consumer.
     *
     * @param groupId the consumer group ID
     */
    public static Properties getConsumerProperties(String groupId) {
        Properties props = getCommonProperties();
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.setProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "10000000");
        props.setProperty(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "10000000");
        props.setProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1");
        props.setProperty(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "500");
        props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "20000");
        props.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "300000");
        return props;
    }

    /**
     * Returns properties configured for a Kafka producer.
     */
    public static Properties getProducerProperties() {
        Properties props = getCommonProperties();
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }

    /**
     * Generates a unique consumer group ID by appending the current timestamp to the base group ID.
     *
     * @param baseGroupId the base consumer group ID
     * @return a unique consumer group ID
     */
    public static String getConsumerGroupId(String baseGroupId) {
        return baseGroupId + "-" + System.currentTimeMillis();
    }
}
