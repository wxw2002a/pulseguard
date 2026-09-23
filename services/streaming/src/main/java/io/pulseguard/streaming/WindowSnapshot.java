package io.pulseguard.streaming;

import org.apache.spark.sql.Row;
import org.bson.Document;

import java.time.Instant;
import java.util.Date;
import java.util.List;

public record WindowSnapshot(String accountId, String currency, Instant windowStart, Instant windowEnd,
                             long transactionCount, long totalAmountMinor, long highValueCount,
                             long smallAmountCount, List<String> transactionIds,
                             List<String> smallAmountTransactionIds) {
    public WindowSnapshot {
        transactionIds = List.copyOf(transactionIds);
        smallAmountTransactionIds = List.copyOf(smallAmountTransactionIds);
    }

    public static WindowSnapshot from(Row row) {
        return new WindowSnapshot(row.getAs("accountId"), row.getAs("currency"),
                row.<java.sql.Timestamp>getAs("windowStart").toInstant(),
                row.<java.sql.Timestamp>getAs("windowEnd").toInstant(),
                row.getAs("transactionCount"), row.getAs("totalAmountMinor"),
                row.getAs("highValueCount"), row.getAs("smallAmountCount"),
                row.getList(row.fieldIndex("transactionIds")),
                row.getList(row.fieldIndex("smallAmountTransactionIds")));
    }

    public String id() {
        return accountId + ":" + currency + ":" + windowStart.toEpochMilli();
    }

    public Document document(Instant updatedAt) {
        return new Document("_id", id()).append("accountId", accountId).append("currency", currency)
                .append("windowStart", Date.from(windowStart)).append("windowEnd", Date.from(windowEnd))
                .append("transactionCount", transactionCount).append("totalAmountMinor", totalAmountMinor)
                .append("highValueCount", highValueCount).append("smallAmountCount", smallAmountCount)
                .append("updatedAt", Date.from(updatedAt));
    }
}
