package io.pulseguard.api.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.time.Duration;
import java.time.Instant;
import com.mongodb.client.result.UpdateResult;
import io.pulseguard.api.transaction.TransactionDocument;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

class OutboxStoreTest {
    private final MongoTemplate mongo = mock(MongoTemplate.class);
    private final OutboxStore store = new OutboxStore(mongo);
    private final Instant now = Instant.parse("2026-09-13T12:00:00Z");

    @Test
    void claimsBothPendingAndExpiredLeasesWithANewOwner() {
        store.claim(now, Duration.ofSeconds(30));
        ArgumentCaptor<Query> query = ArgumentCaptor.forClass(Query.class);
        ArgumentCaptor<Update> update = ArgumentCaptor.forClass(Update.class);
        verify(mongo).findAndModify(query.capture(), update.capture(), any(FindAndModifyOptions.class), eq(TransactionDocument.class));
        assertThat(query.getValue().getQueryObject().toString()).contains("PENDING", "IN_FLIGHT", "leaseUntil", "$lte");
        var fields = update.getValue().getUpdateObject().get("$set", org.bson.Document.class);
        assertThat(fields.getString("outbox.leaseOwner")).isNotBlank();
        assertThat(fields.get("outbox.leaseUntil")).isEqualTo(now.plusSeconds(30));
    }

    @Test
    void staleAcknowledgementCannotMarkReassignedLeaseAsSent() {
        when(mongo.updateFirst(any(Query.class), any(Update.class), eq(TransactionDocument.class)))
                .thenReturn(UpdateResult.acknowledged(0, 0L, null));
        TransactionDocument claimed = claimed("old-owner", 1);
        assertThat(store.markPublished(claimed, now)).isFalse();
        ArgumentCaptor<Query> query = ArgumentCaptor.forClass(Query.class);
        verify(mongo).updateFirst(query.capture(), any(Update.class), eq(TransactionDocument.class));
        assertThat(query.getValue().getQueryObject()).containsEntry("_id", "tx-1")
                .containsEntry("outbox.status", "IN_FLIGHT").containsEntry("outbox.leaseOwner", "old-owner");
    }

    @Test
    void failedDeliveryUsesCappedBackoffAndRetainsLeaseOwnershipGuard() {
        when(mongo.updateFirst(any(Query.class), any(Update.class), eq(TransactionDocument.class)))
                .thenReturn(UpdateResult.acknowledged(1, 1L, null));
        assertThat(store.retry(claimed("owner", 30), now, new IllegalStateException("broker unavailable"))).isTrue();
        ArgumentCaptor<Update> update = ArgumentCaptor.forClass(Update.class);
        verify(mongo).updateFirst(any(Query.class), update.capture(), eq(TransactionDocument.class));
        var fields = update.getValue().getUpdateObject().get("$set", org.bson.Document.class);
        assertThat(fields.get("outbox.nextAttemptAt")).isEqualTo(now.plusSeconds(60));
        assertThat(fields.getString("outbox.status")).isEqualTo("PENDING");
    }

    private TransactionDocument claimed(String owner, int attempts) {
        return new TransactionDocument("tx-1", null, now,
                new TransactionDocument.Outbox("IN_FLIGHT", attempts, now, owner, now.plusSeconds(30), null, null));
    }
}
