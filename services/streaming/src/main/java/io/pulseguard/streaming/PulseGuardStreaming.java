package io.pulseguard.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryListener;
import org.apache.spark.sql.streaming.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class PulseGuardStreaming {
    private static final Logger LOG = LoggerFactory.getLogger(PulseGuardStreaming.class);

    private PulseGuardStreaming() {}

    public static void main(String[] args) throws Exception {
        String bootstrapServers = env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        String transactionTopic = env("TRANSACTION_TOPIC", "transactions.v1");
        String mongoUri = env("MONGODB_URI", "mongodb://localhost:27017/pulseguard");
        String checkpoint = env("CHECKPOINT_DIR", "/var/lib/pulseguard/checkpoints").replaceAll("/+$", "");
        long maxOffsets = Long.parseLong(env("MAX_OFFSETS_PER_TRIGGER", "5000"));
        if (maxOffsets < 1) throw new IllegalArgumentException("MAX_OFFSETS_PER_TRIGGER must be positive");

        SparkConf config = new SparkConf().setAppName("PulseGuardStreaming");
        if (!config.contains("spark.master")) config.setMaster("local[2]");
        if (!config.contains("spark.sql.shuffle.partitions")) config.set("spark.sql.shuffle.partitions", "4");
        // Speculation adds duplicate Mongo work without helping this bounded IO sink.
        config.set("spark.speculation", "false");
        SparkSession spark = SparkSession.builder().config(config)
                .config("spark.sql.session.timeZone", "UTC")
                .config("spark.sql.ansi.enabled", "true")
                .config("spark.sql.streaming.metricsEnabled", "true")
                .getOrCreate();
        spark.sparkContext().setLogLevel(env("SPARK_LOG_LEVEL", "WARN"));
        spark.streams().addListener(new StreamingQueryListener() {
            @Override public void onQueryStarted(QueryStartedEvent event) {
                LOG.warn("Streaming query started name={} id={}", event.name(), event.id());
            }
            @Override public void onQueryProgress(QueryProgressEvent event) {
                // Spark's JSON includes batch duration, watermark, state rows and dropped-late rows.
                LOG.warn("streaming_progress {}", event.progress().json());
            }
            @Override public void onQueryTerminated(QueryTerminatedEvent event) {
                LOG.warn("Streaming query terminated id={} exception={}", event.id(), event.exception());
            }
        });
        Runtime.getRuntime().addShutdownHook(new Thread(spark::stop, "pulseguard-spark-shutdown"));

        try {
            Dataset<Row> raw = spark.readStream().format("kafka")
                    .option("kafka.bootstrap.servers", bootstrapServers)
                    .option("subscribe", transactionTopic)
                    .option("startingOffsets", "earliest")
                    .option("failOnDataLoss", "true")
                    .option("maxOffsetsPerTrigger", maxOffsets)
                    .load();
            Dataset<Row> decoded = StreamTransforms.decode(raw);
            // Two queries isolate stateless rule/DLQ ingestion from event-time aggregation.
            decoded.writeStream().queryName("pulseguard-events-v1")
                    .option("checkpointLocation", checkpoint + "/events-v1")
                    .trigger(Trigger.ProcessingTime("5 seconds"))
                    .foreachBatch((Dataset<Row> batch, Long batchId) -> MongoSinks.writeEvents(batch, mongoUri))
                    .start();
            StreamTransforms.windows(StreamTransforms.validEvents(decoded))
                    .writeStream().queryName("pulseguard-windows-v1").outputMode("update")
                    .option("checkpointLocation", checkpoint + "/windows-v1")
                    .trigger(Trigger.ProcessingTime("5 seconds"))
                    .foreachBatch((Dataset<Row> batch, Long batchId) -> MongoSinks.writeWindows(batch, mongoUri))
                    .start();
            spark.streams().awaitAnyTermination();
        } finally {
            for (StreamingQuery query : spark.streams().active()) query.stop();
            spark.stop();
        }
    }

    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }
}
