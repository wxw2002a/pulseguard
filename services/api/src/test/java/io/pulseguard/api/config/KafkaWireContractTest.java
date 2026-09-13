package io.pulseguard.api.config;

import static org.assertj.core.api.Assertions.assertThat;
import java.time.Instant;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.pulseguard.api.transaction.TransactionPayload;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;

class KafkaWireContractTest {
    @Test
    @SuppressWarnings("unchecked")
    void kafkaUsesIsoTimestampAndNoJavaTypeHeadersAsRequiredBySpark() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(JacksonAutoConfiguration.class, KafkaAutoConfiguration.class))
                .withUserConfiguration(KafkaProducerConfiguration.class)
                .withPropertyValues("spring.jackson.serialization.write-dates-as-timestamps=false",
                        "spring.kafka.producer.properties.spring.json.add.type.headers=false")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    DefaultKafkaProducerFactory<String, TransactionPayload> factory = context.getBean(DefaultKafkaProducerFactory.class);
                    TransactionPayload payload = new TransactionPayload(1, "tx-wire", "acct-wire", "merchant-wire", 100L,
                            TransactionPayload.Currency.USD, "US", TransactionPayload.Channel.WEB,
                            Instant.parse("2026-09-13T12:00:00.123456789Z"));
                    RecordHeaders headers = new RecordHeaders();
                    byte[] wire = factory.getValueSerializer().serialize("transactions.v1", headers, payload);
                    var json = context.getBean(ObjectMapper.class).readTree(wire);
                    assertThat(json.get("eventTime").isTextual()).isTrue();
                    assertThat(json.get("eventTime").textValue()).isEqualTo("2026-09-13T12:00:00.123456789Z");
                    assertThat(json.get("amountMinor").isIntegralNumber()).isTrue();
                    assertThat(headers.toArray()).isEmpty();
                });
    }
}
