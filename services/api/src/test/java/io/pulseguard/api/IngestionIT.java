package io.pulseguard.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.pulseguard.api.config.PulseGuardProperties;
import io.pulseguard.api.outbox.OutboxPublisher;
import io.pulseguard.api.outbox.OutboxStore;
import io.pulseguard.api.investigation.InvestigationService;
import io.pulseguard.api.transaction.TransactionDocument;
import io.pulseguard.api.transaction.TransactionPayload;
import io.pulseguard.api.transaction.TransactionService;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.web.server.ResponseStatusException;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/** Runs only with -Pintegration; Docker is required and absence is a failure, never a silent skip. */
@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {"pulseguard.outbox.enabled=false", "pulseguard.api-key=integration-key"})
class IngestionIT {
    @Container static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0.16"));
    @Container static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("apache/kafka:3.9.1"));
    @Autowired TestRestTemplate http;
    @Autowired MongoTemplate mongo;
    @Autowired ObjectMapper mapper;
    @Autowired TransactionService service;
    @Autowired OutboxStore store;
    @Autowired KafkaTemplate<String, TransactionPayload> kafka;
    @Autowired Clock clock;
    @Autowired MeterRegistry metrics;
    @Autowired InvestigationService investigations;

    @DynamicPropertySource
    static void configure(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", MONGO::getReplicaSetUrl);
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    @BeforeAll
    static void topic() throws Exception {
        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic("transactions.v1", 3, (short) 1))).all().get(20, TimeUnit.SECONDS);
        }
    }

    @BeforeEach
    void cleanTransactions() {
        mongo.remove(new Query(), TransactionDocument.class);
        mongo.remove(new Query(), "alerts");
    }

    @Test
    void httpIngestionPersistsDurableOutboxThenPublishesOriginalContractToKafka() throws Exception {
        TransactionPayload payload = payload("http-" + UUID.randomUUID(), 1000L);
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("X-API-Key", "integration-key");
        var first = http.postForEntity("/api/v1/transactions", new HttpEntity<>(payload, headers), JsonNode.class);
        assertThat(first.getStatusCode().value()).isEqualTo(202);
        assertThat(first.getBody().get("duplicate").asBoolean()).isFalse();
        var duplicate = http.postForEntity("/api/v1/transactions", new HttpEntity<>(payload, headers), JsonNode.class);
        assertThat(duplicate.getStatusCode().value()).isEqualTo(202);
        assertThat(duplicate.getBody().get("duplicate").asBoolean()).isTrue();
        TransactionPayload changed = new TransactionPayload(1, payload.transactionId(), payload.accountId(), payload.merchantId(),
                1001L, payload.currency(), payload.country(), payload.channel(), payload.eventTime());
        assertThat(http.postForEntity("/api/v1/transactions", new HttpEntity<>(changed, headers), JsonNode.class)
                .getStatusCode().value()).isEqualTo(409);
        assertThat(mongo.count(new Query(), TransactionDocument.class)).isEqualTo(1);
        assertThat(mongo.findById(payload.transactionId(), TransactionDocument.class).outbox().status()).isEqualTo("PENDING");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "it-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class))) {
            consumer.subscribe(List.of("transactions.v1"));
            publisher().publishAvailable();
            List<ConsumerRecord<String, String>> received = new ArrayList<>();
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                consumer.poll(Duration.ofMillis(200)).forEach(record -> {
                    if (record.value().contains(payload.transactionId())) received.add(record);
                });
                assertThat(received).hasSize(1);
            });
            assertThat(received.get(0).key()).isEqualTo(payload.accountId());
            assertThat(mapper.readTree(received.get(0).value()).get("eventTime").isTextual()).isTrue();
            assertThat(mapper.readValue(received.get(0).value(), TransactionPayload.class)).isEqualTo(payload);
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                    assertThat(mongo.findById(payload.transactionId(), TransactionDocument.class).outbox().status()).isEqualTo("SENT"));
        }
        JsonNode list = http.getForObject("/api/v1/transactions", JsonNode.class);
        assertThat(list.get("items").get(0).has("outbox")).isFalse();
        assertThat(list.get("items").get(0).get("deliveryStatus").asText()).isEqualTo("SENT");
    }

    @Test
    void concurrentRetriesPersistExactlyOneTransactionAndOneOutbox() {
        TransactionPayload payload = payload("concurrent-1", 2000L);
        var executor = Executors.newFixedThreadPool(8);
        try {
            var submissions = java.util.stream.IntStream.range(0, 24)
                    .mapToObj(index -> CompletableFuture.supplyAsync(() -> service.accept(payload), executor)).toList();
            var responses = submissions.stream().map(CompletableFuture::join).toList();
            assertThat(responses.stream().filter(result -> !result.duplicate()).count()).isEqualTo(1);
            assertThat(mongo.count(new Query(), TransactionDocument.class)).isEqualTo(1);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void expiredLeaseIsReclaimedAndOldOwnerCannotAcknowledgeOrRescheduleIt() {
        service.accept(payload("lease-1", 1000L));
        Instant now = clock.instant().plusSeconds(1);
        TransactionDocument first = store.claim(now, Duration.ZERO);
        TransactionDocument replacement = store.claim(now.plusMillis(1), Duration.ofSeconds(30));
        assertThat(replacement.outbox().leaseOwner()).isNotEqualTo(first.outbox().leaseOwner());
        assertThat(replacement.outbox().attempts()).isEqualTo(2);
        assertThat(store.markPublished(first, now.plusMillis(2))).isFalse();
        assertThat(store.retry(first, now.plusMillis(2), new RuntimeException("late failure"))).isFalse();
        assertThat(store.markPublished(replacement, now.plusMillis(2))).isTrue();
        assertThat(mongo.findById("lease-1", TransactionDocument.class).outbox().status()).isEqualTo("SENT");
    }

    @Test
    void evidenceFindsOnlyOriginalTransactionsForExactAccountCurrencyAndWindow() {
        Instant start = Instant.now().minusSeconds(180).truncatedTo(ChronoUnit.MINUTES);
        Instant end = start.plusSeconds(60);
        service.accept(evidencePayload("at-start", "acct-evidence", TransactionPayload.Currency.CAD, start));
        service.accept(evidencePayload("last-nanosecond", "acct-evidence", TransactionPayload.Currency.CAD, end.minusNanos(1)));
        service.accept(evidencePayload("before-start", "acct-evidence", TransactionPayload.Currency.CAD, start.minusNanos(1)));
        service.accept(evidencePayload("at-end", "acct-evidence", TransactionPayload.Currency.CAD, end));
        service.accept(evidencePayload("wrong-currency", "acct-evidence", TransactionPayload.Currency.USD, start));
        service.accept(evidencePayload("wrong-account", "other-account", TransactionPayload.Currency.CAD, start));
        mongo.insert(new Document("_id", "VELOCITY:window").append("accountId", "acct-evidence").append("currency", "CAD")
                .append("windowStart", Date.from(start)).append("windowEnd", Date.from(end)), "alerts");
        assertThat(investigations.evidence("VELOCITY:window", 200)).extracting(view -> view.transactionId())
                .containsExactly("at-start", "last-nanosecond");
        assertThat(investigations.evidence("VELOCITY:window", 1)).hasSize(1);
        mongo.insert(new Document("_id", "HIGH_VALUE:at-start").append("transactionId", "at-start"), "alerts");
        assertThat(investigations.evidence("HIGH_VALUE:at-start", 200)).extracting(view -> view.transactionId()).containsExactly("at-start");
    }

    @Test
    @SuppressWarnings("unchecked")
    void concurrentReviewsPreserveAllAcceptedNotesAndEnforceCapacityAtomically() {
        List<Document> originalHistory = new ArrayList<>();
        for (int index = 0; index < 499; index++) {
            originalHistory.add(new Document("status", "INVESTIGATING").append("note", "Existing note " + index)
                    .append("analyst", "initial-analyst").append("reviewedAt", new Date()));
        }
        mongo.insert(new Document("_id", "history-cap").append("status", "INVESTIGATING").append("reviewHistory", originalHistory), "alerts");
        var executor = Executors.newFixedThreadPool(8);
        try {
            var attempts = java.util.stream.IntStream.range(0, 8).mapToObj(index -> CompletableFuture.supplyAsync(() -> {
                try {
                    investigations.review("history-cap", InvestigationService.ReviewStatus.RESOLVED, "Concurrent note " + index, "analyst-" + index);
                    return 200;
                } catch (ResponseStatusException exception) {
                    return exception.getStatusCode().value();
                }
            }, executor)).toList();
            var statuses = attempts.stream().map(CompletableFuture::join).toList();
            assertThat(statuses.stream().filter(status -> status == 200).count()).isEqualTo(1);
            assertThat(statuses.stream().filter(status -> status == 409).count()).isEqualTo(7);
        } finally {
            executor.shutdownNow();
        }
        var detail = investigations.detail("history-cap", 500);
        assertThat(detail.get("reviewHistoryCount")).isEqualTo(500);
        List<Map<String, Object>> history = (List<Map<String, Object>>) detail.get("reviewHistory");
        assertThat(history).hasSize(500);
        assertThat(history.get(0).get("note")).isEqualTo("Existing note 0");
        assertThat(history.get(499).get("note").toString()).startsWith("Concurrent note ");
        assertThat((List<?>) investigations.detail("history-cap", 50).get("reviewHistory")).hasSize(50);
        assertThat(investigations.alerts(50, null, null).get(0)).doesNotContainKey("reviewHistory");
    }

    private TransactionPayload evidencePayload(String id, String account, TransactionPayload.Currency currency, Instant eventTime) {
        return new TransactionPayload(1, id, account, "merchant-evidence", 600000L, currency, "CA", TransactionPayload.Channel.WEB, eventTime);
    }

    private OutboxPublisher publisher() {
        return new OutboxPublisher(store, kafka,
                new PulseGuardProperties("integration-key", "transactions.v1", new PulseGuardProperties.Outbox(true, 1000, 50, Duration.ofSeconds(30))),
                clock, metrics);
    }

    private TransactionPayload payload(String id, long amount) {
        return new TransactionPayload(1, id, "acct-integration", "merchant-integration", amount,
                TransactionPayload.Currency.USD, "US", TransactionPayload.Channel.WEB, Instant.now().minusSeconds(1));
    }
}
