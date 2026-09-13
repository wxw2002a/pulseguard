package io.pulseguard.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
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
    void cleanTransactions() { mongo.remove(new Query(), TransactionDocument.class); }

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
