package com.marloncalvo.bench.pipelines;

import com.marloncalvo.bench.common.BrokerEvent;
import com.marloncalvo.bench.common.Service;
import com.marloncalvo.bench.config.ConfigService;

import io.prometheus.metrics.core.metrics.Counter;
import io.prometheus.metrics.core.metrics.Histogram;
import io.prometheus.metrics.exporter.httpserver.HTTPServer;
import io.prometheus.metrics.instrumentation.jvm.JvmMetrics;
import io.prometheus.metrics.model.snapshots.Unit;

import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryListener;
import org.apache.spark.sql.streaming.StreamingQueryProgress;
import org.apache.spark.sql.streaming.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;

public class SparkWebServicePipeline implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(SparkWebServicePipeline.class);
    private static final long serialVersionUID = 1L;

    // Align metrics with WebServicePipeline naming conventions
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
    private final SparkSession spark;
    private final Service executorService;

    public SparkWebServicePipeline() {
        this.inputTopic = System.getenv().getOrDefault("INPUT_TOPIC", "input-topic");
        this.outputTopic = System.getenv().getOrDefault("OUTPUT_TOPIC", "output-topic");
        this.kafkaBootstrapServers = System.getenv().getOrDefault("KAFKA_ENDPOINT", "localhost:9092");

        // Initialize Prometheus metrics server on same port as other pipelines
        JvmMetrics.builder().register();
        try {
            HTTPServer.builder().port(8082).buildAndStart();
            logger.info("Prometheus metrics server started on port 8082");
        } catch (IOException e) {
            logger.error("Failed to start Prometheus metrics server", e);
        }

        // Create Spark session
        this.spark = createOptimizedSparkSession();
        this.executorService = new Service();
    }

    private SparkSession createOptimizedSparkSession() {
        return SparkSession.builder()
                .appName("SparkWebServicePipeline")
                .master(System.getenv().getOrDefault("SPARK_MASTER", "local[*]"))

                // Basic Spark streaming configuration
                .config("spark.sql.streaming.metricsEnabled", "true")
                .config("spark.sql.shuffle.partitions", "32") // Increased for better parallelism

                // Use Java serialization to avoid Kryo version conflicts with Flink
                // Comment: Java serialization is slower but avoids the Kryo/Chill compatibility issues
                .config("spark.serializer", "org.apache.spark.serializer.JavaSerializer")

                .getOrCreate();
    }

    public void run() {
        logger.info("Starting Spark Structured Streaming WebService Pipeline");

        // Register metrics listener for batch monitoring
        spark.streams().addListener(new MetricsListener());

        // Read from Kafka with standard settings
        Dataset<Row> kafkaStream = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaBootstrapServers)
                .option("subscribe", inputTopic)
                .option("startingOffsets", "latest")
                .option("maxOffsetsPerTrigger", "1000000000")
                .option("failOnDataLoss", "false")
                .load();

        // Parse Kafka messages
        Dataset<Row> messages = kafkaStream.selectExpr("timestamp");

        // Execute service calls using map operation
        Dataset<BrokerEvent> serviceResults = messages
                .mapPartitions(
                        (MapPartitionsFunction<Row, BrokerEvent>) (it) -> {
                            var batchSize = ExecutorContext.getClient().getInstance().getBatchSize();
                            ArrayList<CompletableFuture<?>> pending = new ArrayList<>();
                            ArrayList<BrokerEvent> out = new ArrayList<>(pending.size());
                            while (it.hasNext()) {
                                for (int i = 0; i < batchSize && it.hasNext(); i++) {
                                    Row row = it.next();
                                    // Convert java.sql.Timestamp to long (milliseconds)
                                    java.sql.Timestamp timestamp = row.getAs("timestamp");
                                    out.add(new BrokerEvent(timestamp.getTime()));
                                    pending.add(ExecutorContext.getExecutorService().getAsync().exceptionally(ex -> null));
                                    pending.add(ExecutorContext.getExecutorService().getAsync().exceptionally(ex -> null));
                                    pending.add(ExecutorContext.getExecutorService().getAsync().exceptionally(ex -> null));
                                    pending.add(ExecutorContext.getExecutorService().getAsync().exceptionally(ex -> null));
                                }

                                if (!pending.isEmpty()) {
                                    CompletableFuture.allOf(pending.toArray(new CompletableFuture[0])).join();
                                }
                            }
                            return out.iterator();
                        },
                        Encoders.bean(BrokerEvent.class));

        Dataset<Row> kafkaSinkMessage = serviceResults
                .select(functions.to_json(functions.struct(serviceResults.col("*")))
                        .cast("string")
                        .alias("value"));

        // Write to output Kafka topic using native sink for optimal performance
        StreamingQuery query;
        try {
            query = kafkaSinkMessage
                    .writeStream()
                    .format("kafka")
                    .option("kafka.bootstrap.servers", kafkaBootstrapServers)
                    .option("topic", outputTopic)
                    .option("checkpointLocation", "/tmp/spark-checkpoint-" + System.currentTimeMillis())
                    .outputMode("update")
                    .trigger(Trigger.ProcessingTime(1, java.util.concurrent.TimeUnit.MILLISECONDS))
                    .start();
            query.awaitTermination();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // StreamingQueryListener for collecting metrics without impacting performance
    private static class MetricsListener extends StreamingQueryListener {
        @Override
        public void onQueryStarted(StreamingQueryListener.QueryStartedEvent event) {
            logger.info("Query started: {}", event.id());
        }

        @Override
        public void onQueryProgress(StreamingQueryListener.QueryProgressEvent event) {
            StreamingQueryProgress progress = event.progress();

            // Extract batch metrics from Spark's internal monitoring
            long batchId = progress.batchId();
            long numInputRows = progress.numInputRows();

            // Get timing information
            long triggerExecutionTime = 0;
            if (progress.durationMs().containsKey("triggerExecution")) {
                triggerExecutionTime = progress.durationMs().get("triggerExecution");
            }

            // Update Prometheus metrics
            if (numInputRows > 0) {
                batchSizeMetric.observe(numInputRows);
                numberOfBatchesMetric.inc();

                if (triggerExecutionTime > 0) {
                    batchProcessingTime.observe(triggerExecutionTime / 1000.0);
                }

                logger.debug("Processed batch {} with {} records in {} ms",
                        batchId, numInputRows, triggerExecutionTime);
            }

            // Log additional performance metrics available from Spark
            if (progress.inputRowsPerSecond() > 0) {
                logger.debug("Processing rate: {} rows/sec, Batch duration: {} ms",
                        progress.processedRowsPerSecond(),
                        progress.batchDuration());
            }
        }

        @Override
        public void onQueryTerminated(StreamingQueryListener.QueryTerminatedEvent event) {
            logger.info("Query terminated: {}", event.id());
            if (event.exception().isDefined()) {
                logger.error("Query terminated with error", event.exception().get());
            }
        }
    }

    public static void main(String[] args) {
        new SparkWebServicePipeline().run();
    }

    public class ExecutorContext {
        private static transient ConfigService client = null;
        private static transient Service executorService = null;

        public static ConfigService getClient() {
            if (client == null) {
                synchronized (ExecutorContext.class) {
                    if (client == null) {
                        client = new ConfigService(); // expensive init here
                    }
                }
            }
            return client;
        }

        public static Service getExecutorService() {
            if (executorService == null) {
                synchronized (ExecutorContext.class) {
                    if (executorService == null) {
                        executorService = new Service(); // expensive init here
                    }
                }
            }
            return executorService;
        }
    }
}