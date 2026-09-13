package io.pulseguard.streaming;

import com.mongodb.ConnectionString;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.bson.Document;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

/** Executor-side sinks. Every write is replay-safe; a failed write fails its Spark batch. */
public final class MongoSinks {
    private static final int BULK_SIZE = 500;

    private MongoSinks() {}

    public static void writeEvents(Dataset<Row> batch, String mongoUri) {
        batch.foreachPartition((ForeachPartitionFunction<Row>) rows -> writeEventPartition(rows, mongoUri));
    }

    public static void writeWindows(Dataset<Row> batch, String mongoUri) {
        batch.foreachPartition((ForeachPartitionFunction<Row>) rows -> writeWindowPartition(rows, mongoUri));
    }

    private static void writeEventPartition(Iterator<Row> rows, String uri) {
        if (!rows.hasNext()) return;
        try (MongoClient client = MongoClients.create(uri)) {
            MongoDatabase db = database(client, uri);
            List<WriteModel<Document>> alerts = new ArrayList<>(BULK_SIZE);
            List<WriteModel<Document>> deadLetters = new ArrayList<>(BULK_SIZE);
            while (rows.hasNext()) {
                Row row = rows.next();
                Instant now = Instant.now();
                String error = row.getAs("invalidReason");
                if (error != null) {
                    Document deadLetter = new Document("_id", row.getAs("topic") + ":" + row.getAs("partition") + ":" + row.getAs("offset"))
                            .append("topic", row.getAs("topic")).append("partition", row.getAs("partition"))
                            .append("offset", row.getAs("offset")).append("reason", error)
                            .append("payloadExcerpt", row.getAs("rawPayload")).append("createdAt", Date.from(now));
                    deadLetters.add(insertOnce(deadLetter));
                } else {
                    TransactionEvent event = new TransactionEvent(row.getAs("transactionId"), row.getAs("accountId"),
                            row.getAs("merchantId"), row.getAs("amountMinor"), row.getAs("currency"),
                            row.getAs("country"), row.getAs("channel"), row.<Timestamp>getAs("eventTime").toInstant());
                    RiskRules.eventAlerts(event, now).forEach(alert -> alerts.add(insertOnce(alert)));
                }
                if (alerts.size() >= BULK_SIZE) flush(db.getCollection("alerts"), alerts);
                if (deadLetters.size() >= BULK_SIZE) flush(db.getCollection("dead_letters"), deadLetters);
            }
            flush(db.getCollection("alerts"), alerts);
            flush(db.getCollection("dead_letters"), deadLetters);
        }
    }

    private static void writeWindowPartition(Iterator<Row> rows, String uri) {
        if (!rows.hasNext()) return;
        try (MongoClient client = MongoClients.create(uri)) {
            MongoDatabase db = database(client, uri);
            List<WriteModel<Document>> windows = new ArrayList<>(BULK_SIZE);
            List<WriteModel<Document>> alerts = new ArrayList<>(BULK_SIZE);
            while (rows.hasNext()) {
                WindowSnapshot window = WindowSnapshot.from(rows.next());
                Instant now = Instant.now();
                // Full absolute snapshots, never $inc: replaying a committed write cannot double money.
                windows.add(new ReplaceOneModel<>(Filters.eq("_id", window.id()), window.document(now),
                        new ReplaceOptions().upsert(true)));
                RiskRules.windowAlerts(window, now).forEach(alert -> alerts.add(insertOnce(alert)));
                if (windows.size() >= BULK_SIZE) flush(db.getCollection("windows"), windows);
                if (alerts.size() >= BULK_SIZE) flush(db.getCollection("alerts"), alerts);
            }
            flush(db.getCollection("windows"), windows);
            flush(db.getCollection("alerts"), alerts);
        }
    }

    static UpdateOneModel<Document> insertOnce(Document document) {
        return new UpdateOneModel<>(Filters.eq("_id", document.get("_id")),
                new Document("$setOnInsert", document), new UpdateOptions().upsert(true));
    }

    private static MongoDatabase database(MongoClient client, String uri) {
        String databaseName = new ConnectionString(uri).getDatabase();
        return client.getDatabase(databaseName == null ? "pulseguard" : databaseName)
                .withWriteConcern(WriteConcern.MAJORITY);
    }

    private static void flush(MongoCollection<Document> collection, List<WriteModel<Document>> operations) {
        if (operations.isEmpty()) return;
        collection.bulkWrite(operations, new BulkWriteOptions().ordered(false));
        operations.clear();
    }
}
