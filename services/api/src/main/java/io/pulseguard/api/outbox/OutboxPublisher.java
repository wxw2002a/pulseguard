package io.pulseguard.api.outbox;

import java.time.Clock;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.pulseguard.api.config.PulseGuardProperties;
import io.pulseguard.api.transaction.TransactionDocument;
import io.pulseguard.api.transaction.TransactionPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(prefix = "pulseguard.outbox", name = "enabled", havingValue = "true", matchIfMissing = true)
public class OutboxPublisher {
    private static final Logger LOG = LoggerFactory.getLogger(OutboxPublisher.class);
    private final OutboxStore store;
    private final KafkaTemplate<String, TransactionPayload> kafka;
    private final PulseGuardProperties properties;
    private final Clock clock;
    private final Counter published;
    private final Counter retries;

    public OutboxPublisher(OutboxStore store, KafkaTemplate<String, TransactionPayload> kafka,
                           PulseGuardProperties properties, Clock clock, MeterRegistry registry) {
        this.store = store;
        this.kafka = kafka;
        this.properties = properties;
        this.clock = clock;
        published = registry.counter("pulseguard.outbox.published");
        retries = registry.counter("pulseguard.outbox.retries");
    }

    @Scheduled(fixedDelayString = "${pulseguard.outbox.poll-interval-ms:1000}")
    public void publishAvailable() {
        for (int index = 0; index < properties.outbox().batchSize(); index++) {
            TransactionDocument claimed = store.claim(clock.instant(), properties.outbox().leaseDuration());
            if (claimed == null) return;
            if (claimed.outbox().attempts() > 1) retries.increment();
            try {
                kafka.send(properties.transactionTopic(), claimed.payload().accountId(), claimed.payload())
                        .whenComplete((result, failure) -> complete(claimed, failure));
            } catch (Exception exception) {
                complete(claimed, exception);
            }
        }
    }

    private void complete(TransactionDocument claimed, Throwable failure) {
        try {
            if (failure == null) {
                if (store.markPublished(claimed, clock.instant())) published.increment();
            } else {
                store.retry(claimed, clock.instant(), failure);
                LOG.warn("Kafka delivery failed for transaction {}; durable outbox will retry", claimed.id());
            }
        } catch (RuntimeException exception) {
            // The persisted lease expires even if Mongo becomes unavailable during acknowledgement.
            LOG.warn("Could not update delivery state for transaction {}; lease recovery will retry", claimed.id(), exception);
        }
    }
}
