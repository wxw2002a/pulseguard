package io.pulseguard.api.transaction;

import static org.assertj.core.api.Assertions.assertThat;
import java.time.Instant;
import jakarta.validation.Validation;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions;
import org.springframework.data.mongodb.core.convert.NoOpDbRefResolver;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;

class TransactionMappingTest {
    @Test
    void preservesNanosecondEventTimeAcrossMongoRoundTripForIdempotentReplay() throws Exception {
        MongoCustomConversions conversions = MongoCustomConversions.create(adapter -> { });
        MongoMappingContext context = new MongoMappingContext();
        context.setSimpleTypeHolder(conversions.getSimpleTypeHolder());
        context.afterPropertiesSet();
        MappingMongoConverter converter = new MappingMongoConverter(NoOpDbRefResolver.INSTANCE, context);
        converter.setCustomConversions(conversions);
        converter.afterPropertiesSet();
        Instant now = Instant.parse("2026-09-13T12:00:00.123456789Z");
        TransactionDocument transaction = TransactionDocument.create(TransactionServiceTest.payload(100L, now), now);
        Document bson = new Document();
        converter.write(transaction, bson);
        assertThat(bson.get("payload", Document.class).get("eventTime")).isEqualTo(now.toString());
        assertThat(bson.getDate("eventTimeDate")).isEqualTo(java.util.Date.from(now));
        assertThat(converter.read(TransactionDocument.class, bson).payload()).isEqualTo(transaction.payload());
    }

    @Test
    void rejectsInvalidContractFieldsBeforePersistence() {
        try (var factory = Validation.buildDefaultValidatorFactory()) {
            TransactionPayload invalid = new TransactionPayload(2, "invalid/id", "", "merchant", -1L,
                    null, "usa", null, null);
            assertThat(factory.getValidator().validate(invalid)).extracting(violation -> violation.getPropertyPath().toString())
                    .contains("schemaVersion", "transactionId", "accountId", "amountMinor", "currency", "country", "channel", "eventTime");
            assertThat(factory.getValidator().validate(TransactionServiceTest.payload(100L, Instant.now()))).isEmpty();
        }
    }
}
