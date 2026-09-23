package io.pulseguard.streaming;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.model.UpdateOneModel;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

class RiskRulesTest {
    private static final Instant TIME = Instant.parse("2026-01-02T03:04:00Z");

    @Test void highValueBoundaryAndReplayIdentity() {
        assertTrue(RiskRules.eventAlerts(event(499_999), TIME).isEmpty());
        Document first = RiskRules.eventAlerts(event(500_000), TIME).get(0);
        Document replay = RiskRules.eventAlerts(event(500_000), TIME.plusSeconds(30)).get(0);
        assertEquals("HIGH_VALUE:txn1", first.getString("_id"));
        assertEquals(first.getString("_id"), replay.getString("_id"));
        assertEquals("OPEN", first.getString("status"));
        assertEquals(500_000L, first.getLong("amountMinor"));
        assertEquals(1, first.getInteger("evidenceVersion"));
        assertEquals(List.of("txn1"), first.getList("evidenceTransactionIds", String.class));
        assertEquals(1L, first.getLong("evidenceCount"));
        assertFalse(first.getBoolean("evidenceTruncated"));
    }

    @Test void cardTestingPinsOnlyMatchingPaymentsWhileWindowTotalsRemainContext() {
        WindowSnapshot snapshot = new WindowSnapshot("acct1", "CAD", TIME, TIME.plusSeconds(60),
                6, 700_500, 1, 5,
                List.of("large", "small5", "small4", "small3", "small2", "small1"),
                List.of("small5", "small4", "small3", "small2", "small1"));
        List<Document> alerts = RiskRules.windowAlerts(snapshot, TIME);
        Document velocity = alerts.get(0);
        Document cardTesting = alerts.get(1);
        assertEquals(List.of("large", "small1", "small2", "small3", "small4", "small5"),
                velocity.getList("evidenceTransactionIds", String.class));
        assertEquals(List.of("small1", "small2", "small3", "small4", "small5"),
                cardTesting.getList("evidenceTransactionIds", String.class));
        assertEquals(5L, cardTesting.getLong("evidenceCount"));
        assertEquals(6L, cardTesting.getLong("transactionCount"));
        assertEquals(700_500L, cardTesting.getLong("totalAmountMinor"));
        assertFalse(cardTesting.getBoolean("evidenceTruncated"));
    }

    @Test void evidenceIsBoundedSortedAndStableAcrossInputOrderAndReplay() {
        List<String> ascending = IntStream.range(0, 205).mapToObj(index -> "tx%03d".formatted(index)).toList();
        List<String> descending = IntStream.range(0, 205).mapToObj(index -> "tx%03d".formatted(204 - index)).toList();
        WindowSnapshot firstWindow = new WindowSnapshot("acct1", "CAD", TIME, TIME.plusSeconds(60),
                205, 20_500, 0, 205, descending, descending);
        WindowSnapshot replayWindow = new WindowSnapshot("acct1", "CAD", TIME, TIME.plusSeconds(60),
                205, 20_500, 0, 205, ascending, ascending);
        List<Document> firstAlerts = RiskRules.windowAlerts(firstWindow, TIME);
        List<Document> replayAlerts = RiskRules.windowAlerts(replayWindow, TIME.plusSeconds(10));
        for (int index = 0; index < firstAlerts.size(); index++) {
            Document first = firstAlerts.get(index);
            Document replay = replayAlerts.get(index);
            assertEquals(ascending.subList(0, 200), first.getList("evidenceTransactionIds", String.class));
            assertEquals(first.get("evidenceTransactionIds"), replay.get("evidenceTransactionIds"));
            assertEquals(205L, first.getLong("evidenceCount"));
            assertTrue(first.getBoolean("evidenceTruncated"));
            BsonDocument update = MongoSinks.insertOnce(replay).getUpdate()
                    .toBsonDocument(Document.class, MongoClientSettings.getDefaultCodecRegistry());
            assertEquals(1, update.size(), "A replay cannot replace pinned evidence or analyst review fields");
            assertTrue(update.containsKey("$setOnInsert"));
        }
    }

    @Test void velocityAndCardTestingAreSeparateRulesAtInclusiveThreshold() {
        assertTrue(RiskRules.windowAlerts(window(4, 4), TIME).isEmpty());
        assertEquals(List.of("VELOCITY"), RiskRules.windowAlerts(window(5, 4), TIME)
                .stream().map(doc -> doc.getString("rule")).toList());
        List<Document> alerts = RiskRules.windowAlerts(window(5, 5), TIME);
        assertEquals(List.of("VELOCITY", "CARD_TESTING"), alerts.stream().map(doc -> doc.getString("rule")).toList());
        assertNotEquals(alerts.get(0).getString("_id"), alerts.get(1).getString("_id"));
    }

    @Test void replayUpdatesNeverResetAnalystReviewOrIncrementMoney() {
        Document alert = RiskRules.eventAlerts(event(700_000), TIME).get(0);
        UpdateOneModel<Document> operation = MongoSinks.insertOnce(alert);
        BsonDocument update = operation.getUpdate().toBsonDocument(Document.class, MongoClientSettings.getDefaultCodecRegistry());
        assertEquals(1, update.size());
        assertTrue(update.containsKey("$setOnInsert"));
        assertFalse(update.containsKey("$set"));
        assertFalse(update.containsKey("$inc"));
        assertTrue(operation.getOptions().isUpsert());
        assertEquals("HIGH_VALUE:txn1", operation.getFilter()
                .toBsonDocument(Document.class, MongoClientSettings.getDefaultCodecRegistry()).getString("_id").getValue());
    }

    @Test void windowIdsIncludeCurrencyAndStartToAvoidCrossCurrencyMoneySums() {
        WindowSnapshot cad = window(5, 0);
        WindowSnapshot usd = new WindowSnapshot("acct1", "USD", TIME, TIME.plusSeconds(60), 5, 5000, 0, 0,
                cad.transactionIds(), List.of());
        assertNotEquals(cad.id(), usd.id());
        assertEquals(5000L, cad.document(TIME).getLong("totalAmountMinor"));
    }

    private static TransactionEvent event(long amount) {
        return new TransactionEvent("txn1", "acct1", "merchant1", amount, "CAD", "CA", "WEB", TIME);
    }

    private static WindowSnapshot window(long count, long small) {
        List<String> ids = IntStream.range(0, (int) count).mapToObj(index -> "txn" + index).toList();
        return new WindowSnapshot("acct1", "CAD", TIME, TIME.plusSeconds(60), count, 5000, 0, small,
                ids, ids.subList(0, (int) small));
    }
}
