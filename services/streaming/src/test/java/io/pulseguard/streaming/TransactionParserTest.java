package io.pulseguard.streaming;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

class TransactionParserTest {
    private static final Instant KAFKA_TIME = Instant.parse("2026-01-02T03:04:05Z");
    private static final String VALID = """
            {"schemaVersion":1,"transactionId":"txn_1","accountId":"account-1","merchantId":"merchant_1",
             "amountMinor":12345,"currency":"CAD","country":"CA","channel":"WEB","eventTime":"2026-01-02T03:04:05Z"}
            """;

    @Test void preservesExactMoneyAndTimestamp() {
        TransactionEvent event = TransactionParser.parse(VALID, KAFKA_TIME);
        assertEquals(12345L, event.amountMinor());
        assertEquals("account-1", event.accountId());
        assertEquals(KAFKA_TIME, event.eventTime());
    }

    @ParameterizedTest
    @ValueSource(strings = {"0", "-1", "1.0", "\"100\"", "100000000001", "9223372036854775808", "null"})
    void rejectsMoneyOutsideContract(String amount) {
        assertThrows(IllegalArgumentException.class, () -> TransactionParser.parse(
                VALID.replace("12345", amount), KAFKA_TIME));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "[1,2]", "{broken", "null", "{} {}"})
    void rejectsMalformedInputWithoutLeakingPayload(String payload) {
        assertThrows(IllegalArgumentException.class, () -> TransactionParser.parse(payload, KAFKA_TIME));
    }

    @Test void rejectsDuplicateFieldsAndTrailingJson() {
        assertThrows(IllegalArgumentException.class, () -> TransactionParser.parse(
                VALID.replace("\"schemaVersion\":1", "\"schemaVersion\":1,\"schemaVersion\":1"), KAFKA_TIME));
        assertThrows(IllegalArgumentException.class, () -> TransactionParser.parse(VALID + " {}", KAFKA_TIME));
    }

    @Test void enforcesVersionIdentifiersEnumsAndTimestamp() {
        for (String mutated : new String[]{VALID.replace("\"schemaVersion\":1", "\"schemaVersion\":2"),
                VALID.replace("txn_1", "bad:id"), VALID.replace("CAD", "BTC"),
                VALID.replace("\"CA\"", "\"ca\""), VALID.replace("WEB", "BANK"),
                VALID.replace("2026-01-02T03:04:05Z", "2026-01-02")}) {
            assertThrows(IllegalArgumentException.class, () -> TransactionParser.parse(mutated, KAFKA_TIME));
        }
    }

    @Test void acceptsLateEventsButRejectsFuturePoisonUsingStableKafkaTimestamp() {
        assertDoesNotThrow(() -> TransactionParser.parse(VALID, KAFKA_TIME.plusSeconds(86_400)));
        assertDoesNotThrow(() -> TransactionParser.parse(VALID, KAFKA_TIME.minusSeconds(30)));
        assertThrows(IllegalArgumentException.class, () -> TransactionParser.parse(VALID, KAFKA_TIME.minusSeconds(31)));
    }

    @Test void rejectsAncientTimestampOverflowBeforeItCanCrashSpark() {
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> TransactionParser.parse(VALID.replace("2026-01-02T03:04:05Z", "-999999999-01-01T00:00:00Z"), KAFKA_TIME));
        assertEquals("eventTime is outside the supported timestamp range", error.getMessage());
    }
}
