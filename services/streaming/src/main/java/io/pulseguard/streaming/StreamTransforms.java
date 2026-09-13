package io.pulseguard.streaming;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.sql.Timestamp;

import static org.apache.spark.sql.functions.*;

public final class StreamTransforms {
    public static final StructType DECODED_SCHEMA = new StructType()
            .add("topic", DataTypes.StringType, false)
            .add("partition", DataTypes.IntegerType, false)
            .add("offset", DataTypes.LongType, false)
            .add("rawPayload", DataTypes.StringType, true)
            .add("invalidReason", DataTypes.StringType, true)
            .add("transactionId", DataTypes.StringType, true)
            .add("accountId", DataTypes.StringType, true)
            .add("merchantId", DataTypes.StringType, true)
            .add("amountMinor", DataTypes.LongType, true)
            .add("currency", DataTypes.StringType, true)
            .add("country", DataTypes.StringType, true)
            .add("channel", DataTypes.StringType, true)
            .add("eventTime", DataTypes.TimestampType, true);

    private StreamTransforms() {}

    public static Dataset<Row> decode(Dataset<Row> kafka) {
        return kafka.select(col("topic"), col("partition"), col("offset"),
                        col("value").cast("string"), col("timestamp"))
                .map((MapFunction<Row, Row>) StreamTransforms::decodeRow, Encoders.row(DECODED_SCHEMA));
    }

    static Row decodeRow(Row raw) {
        String topic = raw.getString(0);
        int partition = raw.getInt(1);
        long offset = raw.getLong(2);
        String payload = raw.isNullAt(3) ? null : raw.getString(3);
        Timestamp timestamp = raw.isNullAt(4) ? null : raw.getTimestamp(4);
        try {
            TransactionEvent event = TransactionParser.parse(payload, timestamp == null ? null : timestamp.toInstant());
            return RowFactory.create(topic, partition, offset, null, null,
                    event.transactionId(), event.accountId(), event.merchantId(), event.amountMinor(),
                    event.currency(), event.country(), event.channel(), Timestamp.from(event.eventTime()));
        } catch (IllegalArgumentException invalid) {
            String excerpt = payload == null ? null : payload.substring(0, Math.min(payload.length(), 2048));
            return RowFactory.create(topic, partition, offset, excerpt, invalid.getMessage(),
                    null, null, null, null, null, null, null, null);
        }
    }

    public static Dataset<Row> validEvents(Dataset<Row> decoded) {
        return decoded.filter(col("invalidReason").isNull());
    }

    public static Dataset<Row> windows(Dataset<Row> validEvents) {
        // Exactly one stateful operator. collect_set deduplicates the immutable outbox payload
        // across micro-batches while retaining event-time state until the watermark expires.
        return validEvents.withWatermark("eventTime", "2 minutes")
                .groupBy(window(col("eventTime"), "1 minute"), col("accountId"), col("currency"))
                .agg(collect_set(struct(col("transactionId"), col("amountMinor"))).alias("transactions"))
                .select(col("accountId"), col("currency"), col("window.start").alias("windowStart"),
                        col("window.end").alias("windowEnd"),
                        size(col("transactions")).cast("long").alias("transactionCount"),
                        expr("aggregate(transactions, CAST(0 AS BIGINT), (sum, tx) -> sum + tx.amountMinor)")
                                .alias("totalAmountMinor"),
                        expr("CAST(size(filter(transactions, tx -> tx.amountMinor >= 500000)) AS BIGINT)")
                                .alias("highValueCount"),
                        expr("CAST(size(filter(transactions, tx -> tx.amountMinor <= 1000)) AS BIGINT)")
                                .alias("smallAmountCount"));
    }
}
