package io.pulseguard.streaming;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.*;

/** Real Spark runtime and state-store tests. Run on Linux with -Pspark-tests (no Kafka/Mongo required). */
@Tag("spark")
class StreamTransformsTest {
    private static final Instant TIME = Instant.parse("2026-01-02T03:04:05Z");
    private static final ObjectMapper JSON = new ObjectMapper();
    private static SparkSession spark;

    @TempDir Path temporary;

    @BeforeAll static void startSpark() {
        spark = SparkSession.builder().appName("PulseGuardStreamingTests").master("local[2]")
                .config("spark.ui.enabled", "false")
                .config("spark.driver.host", "127.0.0.1")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .config("spark.sql.shuffle.partitions", "2")
                .config("spark.sql.streaming.noDataMicroBatches.enabled", "true")
                .config("spark.sql.session.timeZone", "UTC")
                .config("spark.sql.ansi.enabled", "true").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
    }

    @AfterAll static void stopSpark() {
        if (spark != null) spark.stop();
    }

    @Test void decoderRoutesBadRecordsAndPreservesKafkaCoordinates() throws Exception {
        StructType kafkaSchema = new StructType().add("topic", DataTypes.StringType)
                .add("partition", DataTypes.IntegerType).add("offset", DataTypes.LongType)
                .add("value", DataTypes.StringType).add("timestamp", DataTypes.TimestampType);
        Map<String, Object> valid = event("txn1", "CAD", 12345, TIME);
        valid.put("schemaVersion", 1);
        Dataset<Row> decoded = StreamTransforms.decode(spark.createDataFrame(List.of(
                RowFactory.create("transactions.v1", 2, 42L, JSON.writeValueAsString(valid), Timestamp.from(TIME)),
                RowFactory.create("transactions.v1", 2, 43L, "{broken", Timestamp.from(TIME))), kafkaSchema));
        List<Row> rows = decoded.orderBy("offset").collectAsList();
        assertNull(rows.get(0).getAs("invalidReason"));
        assertEquals(12345L, rows.get(0).<Long>getAs("amountMinor"));
        assertEquals("malformed JSON", rows.get(1).getAs("invalidReason"));
        assertEquals(2, rows.get(1).<Integer>getAs("partition"));
        assertEquals(43L, rows.get(1).<Long>getAs("offset"));
    }

    @Test void batchWindowDeduplicatesExactEventsAndSeparatesCurrencies() throws Exception {
        Path file = temporary.resolve("batch.json");
        write(file, event("txn1", "CAD", 500_000, TIME), event("txn1", "CAD", 500_000, TIME),
                event("txn2", "CAD", 300, TIME), event("txn3", "USD", 100, TIME));
        Dataset<Row> events = spark.read().schema(StreamTransforms.DECODED_SCHEMA).json(file.toString());
        List<Row> windows = StreamTransforms.windows(events).orderBy("currency").collectAsList();
        assertEquals(2, windows.size());
        WindowSnapshot cad = WindowSnapshot.from(windows.get(0));
        assertEquals(2L, cad.transactionCount());
        assertEquals(500_300L, cad.totalAmountMinor());
        assertEquals(1L, cad.highValueCount());
        assertEquals(1L, cad.smallAmountCount());
        assertEquals(Instant.parse("2026-01-02T03:04:00Z"), cad.windowStart());
    }

    @Test void streamingDeduplicatesAcrossBatchesAndCheckpointRestartThenDropsLateWindow() throws Exception {
        Path input = Files.createDirectory(temporary.resolve("input"));
        Path checkpoint = temporary.resolve("checkpoint");
        Map<String, WindowSnapshot> snapshots = new ConcurrentHashMap<>();
        StreamingQuery query = startWindowQuery(input, checkpoint, snapshots);
        String id = "account1:CAD:" + Instant.parse("2026-01-02T03:04:00Z").toEpochMilli();
        try {
            write(input.resolve("01.json"), event("t1", "CAD", 100, TIME), event("t1", "CAD", 100, TIME),
                    event("t2", "CAD", 100, TIME), event("t3", "CAD", 100, TIME), event("t4", "CAD", 100, TIME));
            query.processAllAvailable();
            assertEquals(4L, snapshots.get(id).transactionCount());
            write(input.resolve("02.json"), event("t1", "CAD", 100, TIME), event("t5", "CAD", 200, TIME));
            query.processAllAvailable();
            assertEquals(5L, snapshots.get(id).transactionCount());
            assertEquals(600L, snapshots.get(id).totalAmountMinor());
            assertEquals(2, RiskRules.windowAlerts(snapshots.get(id), TIME).size());

            query.stop();
            query = startWindowQuery(input, checkpoint, snapshots);
            write(input.resolve("03.json"), event("t5", "CAD", 200, TIME), event("t6", "CAD", 300, TIME));
            query.processAllAvailable();
            assertEquals(6L, snapshots.get(id).transactionCount());
            assertEquals(900L, snapshots.get(id).totalAmountMinor());

            // A new event advances event-time. Old windows are evicted after a no-data batch.
            write(input.resolve("04.json"), event("future", "CAD", 100, TIME.plusSeconds(300)));
            query.processAllAvailable();
            write(input.resolve("05.json"), event("too-late", "CAD", 999, TIME));
            query.processAllAvailable();
            assertEquals(6L, snapshots.get(id).transactionCount(), "late record must not reopen expired state");
            assertEquals(900L, snapshots.get(id).totalAmountMinor());
        } finally {
            query.stop();
        }
    }

    private static StreamingQuery startWindowQuery(Path input, Path checkpoint, Map<String, WindowSnapshot> snapshots)
            throws Exception {
        Dataset<Row> events = spark.readStream().schema(StreamTransforms.DECODED_SCHEMA).json(input.toString());
        return StreamTransforms.windows(StreamTransforms.validEvents(events)).writeStream().outputMode("update")
                .option("checkpointLocation", checkpoint.toString())
                .foreachBatch((Dataset<Row> batch, Long batchId) -> {
                    // Tests intentionally collect small bounded fixtures; production sinks never collect.
                    for (Row row : batch.collectAsList()) {
                        WindowSnapshot snapshot = WindowSnapshot.from(row);
                        snapshots.put(snapshot.id(), snapshot);
                    }
                }).start();
    }

    @SafeVarargs private static void write(Path path, Map<String, Object>... records) throws Exception {
        List<String> lines = new ArrayList<>();
        for (Map<String, Object> record : records) lines.add(JSON.writeValueAsString(record));
        // Atomic publication prevents a running file source from observing a partial fixture.
        Path temporaryFile = path.resolveSibling("." + path.getFileName() + ".tmp");
        Files.write(temporaryFile, lines);
        Files.move(temporaryFile, path, StandardCopyOption.ATOMIC_MOVE);
    }

    private static Map<String, Object> event(String id, String currency, long amount, Instant time) {
        Map<String, Object> record = new LinkedHashMap<>();
        record.put("topic", "transactions.v1"); record.put("partition", 0); record.put("offset", 1L);
        record.put("transactionId", id); record.put("accountId", "account1"); record.put("merchantId", "merchant1");
        record.put("currency", currency); record.put("amountMinor", amount);
        record.put("country", "CA"); record.put("channel", "WEB"); record.put("eventTime", time.toString());
        return record;
    }
}
