package com.marloncalvo.bench.pipelines;

import com.marloncalvo.bench.common.KafkaConfig;

public class SimplePipeline extends BasePipeline {

    public static void main(String[] args) {
        new SimplePipeline().Run();
    }

    @Override
    protected void Run() {
        try (
            var consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<
                String,
                String
            >(
                KafkaConfig.getConsumerProperties(
                    KafkaConfig.getConsumerGroupId(this.getConsumerGroupId())
                )
            );
            var producer = new org.apache.kafka.clients.producer.KafkaProducer<
                String,
                String
            >(KafkaConfig.getProducerProperties())
        ) {
            consumer.subscribe(
                java.util.Collections.singletonList(this.getInputTopic())
            );

            while (true) {
                var records = consumer.poll(java.time.Duration.ofMillis(100));
                for (var record : records) {
                    long startTime = System.nanoTime();
                    var timestamp = record.timestamp();
                    producer.send(
                        new org.apache.kafka.clients.producer.ProducerRecord<>(
                            this.getOutputTopic(),
                            record.key(),
                            this.getKafkaEventSerialized(timestamp)
                        )
                    );
                    long duration = System.nanoTime() - startTime;
                    this.observeProcessingTime(duration);
                }
            }
        } catch (Exception e) {
            logger.error("Error in pipeline", e);
        }
    }
}
