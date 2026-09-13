package io.pulseguard.api.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.CompletableFuture;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.pulseguard.api.config.PulseGuardProperties;
import io.pulseguard.api.transaction.TransactionDocument;
import io.pulseguard.api.transaction.TransactionPayload;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

class OutboxPublisherTest {
    private final OutboxStore store = mock(OutboxStore.class);
    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, TransactionPayload> kafka = mock(KafkaTemplate.class);
    private final Instant now = Instant.parse("2026-09-13T12:00:00Z");
    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final OutboxPublisher publisher = new OutboxPublisher(store, kafka,
            new PulseGuardProperties("key", "transactions.v1", new PulseGuardProperties.Outbox(true, 1000, 1, Duration.ofSeconds(30))),
            Clock.fixed(now, ZoneOffset.UTC), registry);

    @Test
    void doesNotMarkSentUntilKafkaAcknowledges() {
        TransactionDocument claimed = claimed(1);
        when(store.claim(any(), any())).thenReturn(claimed);
        CompletableFuture<SendResult<String, TransactionPayload>> delivery = new CompletableFuture<>();
        when(kafka.send("transactions.v1", "acct-1", claimed.payload())).thenReturn(delivery);
        when(store.markPublished(claimed, now)).thenReturn(true);
        publisher.publishAvailable();
        verify(store, never()).markPublished(any(), any());
        delivery.complete(null);
        verify(store).markPublished(claimed, now);
        assertThat(registry.get("pulseguard.outbox.published").counter().count()).isEqualTo(1);
    }

    @Test
    void deliveryFailureSchedulesRetryAndNeverMarksSent() {
        TransactionDocument claimed = claimed(2);
        when(store.claim(any(), any())).thenReturn(claimed);
        RuntimeException failure = new RuntimeException("broker unavailable");
        when(kafka.send("transactions.v1", "acct-1", claimed.payload())).thenReturn(CompletableFuture.failedFuture(failure));
        publisher.publishAvailable();
        verify(store).retry(claimed, now, failure);
        verify(store, never()).markPublished(any(), any());
        assertThat(registry.get("pulseguard.outbox.retries").counter().count()).isEqualTo(1);
    }

    @Test
    void staleCallbackIsNotCountedAsSuccessfulPublication() {
        TransactionDocument claimed = claimed(1);
        when(store.claim(any(), any())).thenReturn(claimed);
        when(kafka.send("transactions.v1", "acct-1", claimed.payload())).thenReturn(CompletableFuture.completedFuture(null));
        when(store.markPublished(claimed, now)).thenReturn(false);
        publisher.publishAvailable();
        assertThat(registry.get("pulseguard.outbox.published").counter().count()).isZero();
    }

    private TransactionDocument claimed(int attempts) {
        TransactionPayload payload = new TransactionPayload(1, "tx-1", "acct-1", "merchant", 100L,
                TransactionPayload.Currency.USD, "US", TransactionPayload.Channel.WEB, now);
        return new TransactionDocument("tx-1", payload, now, now,
                new TransactionDocument.Outbox("IN_FLIGHT", attempts, now, "owner", now.plusSeconds(30), null, null));
    }
}
