package com.marloncalvo.bench.pipelines;

import com.marloncalvo.bench.common.Buffer;
import com.marloncalvo.bench.common.KafkaConfig;
import com.marloncalvo.bench.config.ConfigService;
import io.prometheus.metrics.core.metrics.Counter;
import io.prometheus.metrics.core.metrics.Histogram;
import io.prometheus.metrics.model.snapshots.Unit;
import java.util.concurrent.Callable;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

public class WebServicePipeline extends BasePipeline {

    private static final Histogram batchProcessingTime = Histogram.builder()
            .name("batch_process_time_seconds")
            .help("Time taken for batch processing")
            .unit(Unit.SECONDS)
            .classicExponentialUpperBounds(0.00001, 1.2, 120)
            .register();

    private static final Histogram kafkaBatchProcessingTime = Histogram.builder()
            .name("kafkabatch_process_time_seconds")
            .help("Time taken for batch processing")
            .unit(Unit.SECONDS)
            .classicExponentialUpperBounds(0.00001, 1.2, 120)
            .register();

    private static final Histogram batchSizeMetric = Histogram.builder()
            .name("batch_size")
            .help("Number of records processed in each batch")
            .classicUpperBounds(
                    1,
                    10,
                    50,
                    100,
                    500,
                    1000,
                    5000,
                    10000,
                    50000,
                    100000)
            .register();

    private static final Counter numberOfBatchesMetric = Counter.builder()
            .name("number_of_batches")
            .help("Number of batches processed in each poll")
            .register();

    public static void main(String[] args) {
        new WebServicePipeline().Run();
    }

    @Override
    protected void Run() {
        var configService = new ConfigService();

        try (
                var consumer = new KafkaConsumer<String, String>(
                        KafkaConfig.getConsumerProperties(this.getConsumerGroupId()));
                var producer = new KafkaProducer<String, String>(
                        KafkaConfig.getProducerProperties())) {
            consumer.subscribe(
                    java.util.Collections.singletonList(this.getInputTopic()));
            logger.info("WebServicePipeline started.");

            while (true) {
                var semaphore = configService
                        .getInstance()
                        .getWebServiceSemaphore();

                var records = consumer.poll(java.time.Duration.ofMillis(100));
                if (records.isEmpty()) {
                    continue;
                }

                var recordsIter = records.iterator();
                var events = new Buffer<Callable<Boolean>>(
                        configService.getInstance().getBatchSize());

                int recordCount = 0;
                long kafkaStartTime = System.currentTimeMillis();
                while (recordsIter.hasNext()) {
                    long batchStartTime = System.currentTimeMillis();
                    try {
                        while (!events.isFull() && recordsIter.hasNext()) {
                            recordCount++;
                            var record = recordsIter.next();
                            events.add(() -> processRecord(record, semaphore, producer));
                        }
                        var futures = getExecutorService().invokeAll(events);
                        for (var f : futures) {
                            f.get();
                        }
                    } catch (Exception e) {
                        logger.error("Error processing batch", e);
                    } finally {
                        long batchDuration = System.currentTimeMillis() - batchStartTime;
                        events.clear();
                        numberOfBatchesMetric.inc();
                        batchProcessingTime.observe(batchDuration / 1000.0);
                    }
                }

                long kafkaDuration = System.currentTimeMillis() - kafkaStartTime;
                batchSizeMetric.observe(recordCount);
                kafkaBatchProcessingTime.observe(kafkaDuration / 1000.0);
            }
        } catch (Exception e) {
            logger.error("Error in pipeline", e);
        }
    }

    private boolean processRecord(
            ConsumerRecord<String, String> record,
            java.util.concurrent.Semaphore semaphore,
            KafkaProducer<String, String> producer) throws Exception {
        long startTime = System.currentTimeMillis();
        semaphore.acquire();
        try {

            var a = getExecutorService().submit(() -> this.serviceEndpointCall());
            var b = getExecutorService().submit(() -> this.serviceEndpointCall());
            var c = getExecutorService().submit(() -> this.serviceEndpointCall());
            var d = getExecutorService().submit(() -> this.serviceEndpointCall());

            a.get();
            b.get();
            c.get();
            d.get();
        } catch (Exception e) {
            logger.error("Error processing message in virtual thread", e);
        } finally {
            long duration = System.currentTimeMillis() - startTime;
            semaphore.release();
            this.observeProcessingTime(duration);
        }

        var timestamp = record.timestamp();
        producer.send(
                new org.apache.kafka.clients.producer.ProducerRecord<>(
                        this.getOutputTopic(),
                        record.key(),
                        this.getKafkaEventSerialized(timestamp)));

        return true;
    }
}
