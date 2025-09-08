package com.marloncalvo.bench.pipelines;

import com.marloncalvo.bench.common.BrokerEvent;
import com.marloncalvo.bench.common.Service;
import com.marloncalvo.bench.config.ConfigService;
import io.prometheus.metrics.core.metrics.Counter;
import io.prometheus.metrics.core.metrics.Histogram;
import io.prometheus.metrics.exporter.httpserver.HTTPServer;
import io.prometheus.metrics.instrumentation.jvm.JvmMetrics;
import io.prometheus.metrics.model.snapshots.Unit;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryListener;
import org.apache.spark.sql.streaming.StreamingQueryProgress;
import org.apache.spark.sql.streaming.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;

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
    }

    private SparkSession createOptimizedSparkSession() {
        return SparkSession.builder()
                .appName("SparkWebServicePipeline")
                .master(System.getenv().getOrDefault("SPARK_MASTER", "local[*]"))
                
                // Basic Spark streaming configuration
                .config("spark.sql.streaming.metricsEnabled", "true")
                .config("spark.sql.shuffle.partitions", "32") // Increased for better parallelism
                
                // Standard serialization for benchmarking
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                
                .getOrCreate();
    }

    public void run() {
        ConfigService configService = new ConfigService();

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
                .option("maxOffsetsPerTrigger", "10000")
                .option("failOnDataLoss", "false")
                .load();

        // Parse Kafka messages
        Dataset<Row> messages = kafkaStream
                .selectExpr(
                    "CAST(key AS STRING) as messageKey",
                    "CAST(value AS STRING) as messageValue", 
                    "timestamp",
                    "offset",
                    "partition")
                .withColumn("tasks", functions.expr("array(1,2,3,4)")); // Create 4 tasks per message
        
        // Expand each message into 4 service call tasks using explode
        Dataset<Row> serviceTasks = messages
                .select(
                    functions.col("messageKey"),
                    functions.col("messageValue"),
                    functions.col("timestamp"),
                    functions.explode(functions.col("tasks")).as("taskId"))
                .repartition(200); // Distribute tasks across 200 partitions for parallelism
        
        // Execute service calls using map operation
        Dataset<Row> serviceResults = serviceTasks
                .map(new ServiceCallProcessor(), Encoders.bean(ServiceCallResult.class))
                .toDF();
        
        // Group results back by original message
        Dataset<Row> processedStream = serviceResults
                .groupBy("messageKey", "timestamp")
                .agg(
                    functions.first("outputValue").as("value"),
                    functions.avg("callDuration").as("processingTime"),
                    functions.count("*").as("serviceCallCount"))
                .select(
                    functions.col("messageKey").as("key"),
                    functions.col("value"));

        // Write to output Kafka topic using native sink for optimal performance
        StreamingQuery query;
        try {
            query = processedStream
                    .selectExpr("key", "value")
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

    // Service call processor - maintains one Service instance per executor
    private static class ServiceCallProcessor implements MapFunction<Row, ServiceCallResult> {
        private static final long serialVersionUID = 1L;
        
        // Lazy-initialized Service instance per executor (JVM)
        private static transient Service executorService;
        
        private Service getService() {
            if (executorService == null) {
                synchronized (ServiceCallProcessor.class) {
                    if (executorService == null) {
                        executorService = new Service();
                        logger.info("Created Service instance for executor");
                    }
                }
            }
            return executorService;
        }
        
        @Override
        public ServiceCallResult call(Row row) throws Exception {
            String messageKey = row.getAs("messageKey");
            java.sql.Timestamp timestamp = row.getAs("timestamp");
            
            long startTime = System.currentTimeMillis();
            ServiceCallResult result = new ServiceCallResult();
            
            try {
                // Make the synchronous service call
                getService().get();
                
                long duration = System.currentTimeMillis() - startTime;
                serviceCallTime.observe(duration / 1000.0);
                recordProcessingTime.observe(duration / 1000.0);
                
                // Create output event
                BrokerEvent event = new BrokerEvent(timestamp.getTime());
                String outputValue = new com.fasterxml.jackson.databind.ObjectMapper()
                        .writeValueAsString(event);
                
                result.messageKey = messageKey;
                result.timestamp = timestamp;
                result.outputValue = outputValue;
                result.callDuration = duration;
                result.success = true;
                
            } catch (Exception e) {
                logger.error("Service call failed", e);
                
                result.messageKey = messageKey;
                result.timestamp = timestamp;
                result.outputValue = "";
                result.callDuration = System.currentTimeMillis() - startTime;
                result.success = false;
            }
            
            return result;
        }
    }
    
    // Result of a service call
    public static class ServiceCallResult implements Serializable {
        private static final long serialVersionUID = 1L;
        public String messageKey;
        public java.sql.Timestamp timestamp;
        public String outputValue;
        public long callDuration;
        public boolean success;
        
        // Getters and setters
        public String getMessageKey() { return messageKey; }
        public void setMessageKey(String messageKey) { this.messageKey = messageKey; }
        public java.sql.Timestamp getTimestamp() { return timestamp; }
        public void setTimestamp(java.sql.Timestamp timestamp) { this.timestamp = timestamp; }
        public String getOutputValue() { return outputValue; }
        public void setOutputValue(String outputValue) { this.outputValue = outputValue; }
        public long getCallDuration() { return callDuration; }
        public void setCallDuration(long callDuration) { this.callDuration = callDuration; }
        public boolean isSuccess() { return success; }
        public void setSuccess(boolean success) { this.success = success; }
    }

    // Message classes
    public static class KafkaMessage implements Serializable {
        private static final long serialVersionUID = 1L;
        public String key;
        public String value;
        public java.sql.Timestamp timestamp;
        public Long offset;
        public Integer partition;

        // Getters and setters
        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }

        public java.sql.Timestamp getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(java.sql.Timestamp timestamp) {
            this.timestamp = timestamp;
        }

        public Long getOffset() {
            return offset;
        }

        public void setOffset(Long offset) {
            this.offset = offset;
        }

        public Integer getPartition() {
            return partition;
        }

        public void setPartition(Integer partition) {
            this.partition = partition;
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
}