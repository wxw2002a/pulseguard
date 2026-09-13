package io.pulseguard.api.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.pulseguard.api.transaction.TransactionPayload;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

@Configuration
public class KafkaProducerConfiguration {
    @Bean
    ProducerFactory<String, TransactionPayload> transactionProducerFactory(KafkaProperties properties, ObjectMapper mapper) {
        // The no-arg Kafka JsonSerializer owns another mapper and can emit numeric Instants.
        // Use the HTTP mapper so Kafka also carries the contract's ISO-8601 eventTime string.
        return new DefaultKafkaProducerFactory<>(properties.buildProducerProperties(null),
                new StringSerializer(), new JsonSerializer<TransactionPayload>(mapper));
    }

    @Bean
    KafkaTemplate<String, TransactionPayload> transactionKafkaTemplate(ProducerFactory<String, TransactionPayload> factory) {
        return new KafkaTemplate<>(factory);
    }
}
