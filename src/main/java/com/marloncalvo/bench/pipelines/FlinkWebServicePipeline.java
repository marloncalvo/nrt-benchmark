package com.marloncalvo.bench.pipelines;

import com.marloncalvo.bench.common.BrokerEvent;
import com.marloncalvo.bench.common.Service;
import io.prometheus.metrics.core.metrics.Counter;
import io.prometheus.metrics.core.metrics.Histogram;
import io.prometheus.metrics.exporter.httpserver.HTTPServer;
import io.prometheus.metrics.instrumentation.jvm.JvmMetrics;
import io.prometheus.metrics.model.snapshots.Unit;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchemaBuilder;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Flink-based WebService Pipeline with optimized JSON handling.
 * 
 * For production use, consider these JSON optimization options:
 * 1. Use Flink's JsonRowSerializationSchema/JsonRowDeserializationSchema for
 * Table API
 * 2. Enable object reuse: env.getConfig().enableObjectReuse()
 * 3. Use Avro or Protobuf for better performance than JSON
 * 4. Consider Flink SQL with JSON format for declarative processing
 */
public class FlinkWebServicePipeline implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(FlinkWebServicePipeline.class);
    private static final long serialVersionUID = 1L;

    // Metrics aligned with WebServicePipeline
    private static final Histogram batchProcessingTime = Histogram.builder()
            .name("batch_process_time_seconds")
            .help("Time taken for batch processing")
            .unit(Unit.SECONDS)
            .classicExponentialUpperBounds(0.00001, 1.2, 120)
            .register();

    private static final Histogram recordProcessingTime = Histogram.builder()
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

    private static final Counter numberOfBatchesMetric = Counter.builder()
            .name("number_of_batches")
            .help("Number of batches processed")
            .register();

    private static final Histogram batchSizeMetric = Histogram.builder()
            .name("batch_size")
            .help("Number of records processed in each batch")
            .classicUpperBounds(1, 10, 50, 100, 500, 1000, 5000, 10000, 50000, 100000)
            .register();

    private final String inputTopic;
    private final String outputTopic;
    private final String kafkaBootstrapServers;
    private final String consumerGroupId;

    public FlinkWebServicePipeline() {
        this.inputTopic = System.getenv().getOrDefault("INPUT_TOPIC", "input-topic");
        this.outputTopic = System.getenv().getOrDefault("OUTPUT_TOPIC", "output-topic");
        this.kafkaBootstrapServers = System.getenv().getOrDefault("KAFKA_ENDPOINT", "localhost:9092");
        this.consumerGroupId = System.getenv().getOrDefault("CONSUMER_GROUP_ID", "flink-pipeline-group");

        // Initialize Prometheus metrics server
        JvmMetrics.builder().register();
        try {
            HTTPServer.builder().port(8082).buildAndStart();
            logger.info("Prometheus metrics server started on port 8082");
        } catch (IOException e) {
            logger.error("Failed to start Prometheus metrics server", e);
        }
    }

    public void run() throws Exception {
        logger.info("Initializing Flink WebService Pipeline");

        // Configure Flink environment (use cluster defaults for REST/UI)
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        logger.info("Flink version:");

        // Set parallelism from env (defaults to 1 if unset)
        env.setParallelism(10);

        // Enable object reuse for better performance with JSON
        env.getConfig().enableObjectReuse();

        // Enable checkpointing for fault tolerance
        // env.enableCheckpointing(60000); // 1 minute checkpoint interval

        logger.info("Starting Flink WebService Pipeline");
        logger.info("Input topic: {}, Output topic: {}, Kafka: {}", inputTopic, outputTopic, kafkaBootstrapServers);

        JsonDeserializationSchema<BrokerEvent> deserializationJsonFormat = new JsonDeserializationSchema<>(
                BrokerEvent.class);
        JsonSerializationSchema<BrokerEvent> serializationJsonFormat = new JsonSerializationSchema<>();

        // Create Kafka source
        KafkaSource<BrokerEvent> source = KafkaSource.<BrokerEvent>builder()
                .setBootstrapServers(kafkaBootstrapServers)
                .setTopics(inputTopic)
                .setGroupId(consumerGroupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(deserializationJsonFormat)
                .build();

        // Create DataStream from source
        DataStream<BrokerEvent> sourceStream = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source"
        );

        // Process messages with async I/O for service calls
        DataStream<BrokerEvent> processedStream = AsyncDataStream
                .unorderedWait(
                        sourceStream,
                        new AsyncServiceCallFunction(),
                        10, // timeout
                        TimeUnit.SECONDS,
                        100 // capacity
                );

        // Create Kafka sink with custom serializer
        KafkaSink<BrokerEvent> sink = KafkaSink.<BrokerEvent>builder()
                .setBootstrapServers(kafkaBootstrapServers)
                .setRecordSerializer(new KafkaRecordSerializationSchemaBuilder<BrokerEvent>()
                        .setTopic(outputTopic)
                        .setValueSerializationSchema(serializationJsonFormat)
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        // Write processed data to Kafka
        processedStream.sinkTo(sink);

        // Execute the Flink job
        env.execute("Flink WebService Pipeline");
    }

    // Async function for making service calls
    private static class AsyncServiceCallFunction extends RichAsyncFunction<BrokerEvent, BrokerEvent> {
        private static final long serialVersionUID = 1L;
        private transient Service service;

        @Override
        public void open(OpenContext openContext) throws Exception {
            service = new Service();
        }

        @Override
        public void close() throws Exception {
            // service.Cl();
        }

        @Override
        public void asyncInvoke(BrokerEvent input, ResultFuture<BrokerEvent> resultFuture) throws Exception {
            long startTime = System.currentTimeMillis();

            // Create 4 concurrent service call futures
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            for (int i = 0; i < 4; i++) {
                futures.add(CompletableFuture.runAsync(() -> {
                    long callStart = System.currentTimeMillis();
                    service.get();
                    long callDuration = System.currentTimeMillis() - callStart;
                    serviceCallTime.observe(callDuration / 1000.0);
                }));
            }

            // Wait for all service calls to complete
            CompletableFuture<Void> allFutures = CompletableFuture.allOf(
                    futures.toArray(new CompletableFuture[0]));

            allFutures.whenComplete((result, throwable) -> {
                try {
                    long duration = System.currentTimeMillis() - startTime;
                    recordProcessingTime.observe(duration / 1000.0);

                    var outputBrokerEvent = new BrokerEvent(input.getBrokerConsumeEventTimestamp());

                    // Complete the async operation
                    resultFuture.complete(Collections.singletonList(outputBrokerEvent));
                } catch (Exception e) {
                    logger.error("Error processing message", e);
                    resultFuture.completeExceptionally(e);
                }
            });
        }

        @Override
        public void timeout(BrokerEvent input, ResultFuture<BrokerEvent> resultFuture) throws Exception {
            logger.warn("Timeout processing message");
            resultFuture.complete(Collections.emptyList());
        }
    }

    public static void main(String[] args) throws Exception {
        new FlinkWebServicePipeline().run();
    }
}
