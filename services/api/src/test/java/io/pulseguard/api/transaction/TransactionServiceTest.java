package io.pulseguard.api.transaction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.web.server.ResponseStatusException;

class TransactionServiceTest {
    private final Instant now = Instant.parse("2026-09-13T12:00:00Z");
    private final MongoTemplate mongo = mock(MongoTemplate.class);
    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final TransactionService service = new TransactionService(mongo, Clock.fixed(now, ZoneOffset.UTC), registry);

    @Test
    void atomicallyInsertsTransactionAndPendingOutbox() {
        var result = service.accept(payload(4999L, now));
        ArgumentCaptor<TransactionDocument> document = ArgumentCaptor.forClass(TransactionDocument.class);
        verify(mongo).insert(document.capture());
        assertThat(result.duplicate()).isFalse();
        assertThat(document.getValue().id()).isEqualTo("tx-001");
        assertThat(document.getValue().outbox().status()).isEqualTo("PENDING");
        assertThat(document.getValue().outbox().nextAttemptAt()).isEqualTo(now);
        assertThat(registry.get("pulseguard.transactions.accepted").counter().count()).isEqualTo(1);
    }

    @Test
    void duplicateSamePayloadDoesNotCreateAnotherDeliveryIntent() {
        TransactionPayload payload = payload(4999L, now);
        when(mongo.insert(any(TransactionDocument.class))).thenThrow(new DuplicateKeyException("duplicate"));
        when(mongo.findById("tx-001", TransactionDocument.class)).thenReturn(TransactionDocument.create(payload, now));
        assertThat(service.accept(payload).duplicate()).isTrue();
        assertThat(registry.get("pulseguard.transactions.duplicates").counter().count()).isEqualTo(1);
    }

    @Test
    void rejectsIdReuseWithChangedBusinessPayload() {
        when(mongo.insert(any(TransactionDocument.class))).thenThrow(new DuplicateKeyException("duplicate"));
        when(mongo.findById("tx-001", TransactionDocument.class)).thenReturn(TransactionDocument.create(payload(4999L, now), now));
        assertThatThrownBy(() -> service.accept(payload(5000L, now)))
                .isInstanceOf(ResponseStatusException.class).satisfies(exception ->
                        assertThat(((ResponseStatusException) exception).getStatusCode().value()).isEqualTo(409));
    }

    @Test
    void futureClockGuardPreventsWatermarkPoisoning() {
        assertThatThrownBy(() -> service.accept(payload(4999L, now.plusSeconds(31))))
                .isInstanceOf(ResponseStatusException.class);
        verifyNoInteractions(mongo);
    }

    @Test
    void rejectsTimestampThatWouldOverflowSparkMicroseconds() {
        assertThatThrownBy(() -> service.accept(payload(4999L, Instant.parse("-999999999-01-01T00:00:00Z"))))
                .isInstanceOf(ResponseStatusException.class);
        verifyNoInteractions(mongo);
    }

    static TransactionPayload payload(long amount, Instant time) {
        return new TransactionPayload(1, "tx-001", "acct-001", "merchant-001", amount,
                TransactionPayload.Currency.USD, "US", TransactionPayload.Channel.WEB, time);
    }
}
