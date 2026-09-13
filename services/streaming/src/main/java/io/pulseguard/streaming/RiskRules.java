package io.pulseguard.streaming;

import org.bson.Document;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/** Transparent demonstration rules; the score is a fixed rule weight, not a probability. */
public final class RiskRules {
    public static final long HIGH_VALUE_THRESHOLD = 500_000;
    public static final long VELOCITY_THRESHOLD = 5;

    private RiskRules() {}

    public static List<Document> eventAlerts(TransactionEvent event, Instant observedAt) {
        if (event.amountMinor() < HIGH_VALUE_THRESHOLD) return List.of();
        return List.of(base("HIGH_VALUE:" + event.transactionId(), "HIGH_VALUE", "HIGH", 90,
                event.accountId(), event.currency(), event.eventTime(), observedAt)
                .append("transactionId", event.transactionId())
                .append("amountMinor", event.amountMinor())
                .append("reasons", List.of("Amount is at least 500000 minor units (5000 currency units)")));
    }

    public static List<Document> windowAlerts(WindowSnapshot snapshot, Instant observedAt) {
        List<Document> alerts = new ArrayList<>();
        if (snapshot.transactionCount() >= VELOCITY_THRESHOLD) {
            alerts.add(windowAlert(snapshot, observedAt, "VELOCITY", "MEDIUM", 75,
                    "At least 5 distinct transactions for one account and currency within a UTC minute"));
        }
        if (snapshot.smallAmountCount() >= VELOCITY_THRESHOLD) {
            alerts.add(windowAlert(snapshot, observedAt, "CARD_TESTING", "HIGH", 85,
                    "At least 5 distinct transactions of at most 1000 minor units within a UTC minute"));
        }
        return alerts;
    }

    private static Document windowAlert(WindowSnapshot snapshot, Instant observedAt, String rule,
                                        String severity, int score, String reason) {
        return base(rule + ":" + snapshot.id(), rule, severity, score, snapshot.accountId(),
                snapshot.currency(), snapshot.windowEnd(), observedAt)
                .append("windowStart", Date.from(snapshot.windowStart()))
                .append("windowEnd", Date.from(snapshot.windowEnd()))
                .append("transactionCount", snapshot.transactionCount())
                .append("totalAmountMinor", snapshot.totalAmountMinor())
                .append("reasons", List.of(reason));
    }

    private static Document base(String id, String rule, String severity, int score,
                                 String accountId, String currency, Instant eventTime, Instant observedAt) {
        return new Document("_id", id).append("rule", rule).append("severity", severity)
                .append("score", score).append("accountId", accountId).append("currency", currency)
                .append("eventTime", Date.from(eventTime)).append("createdAt", Date.from(observedAt))
                .append("status", "OPEN");
    }
}
