package io.pulseguard.streaming;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Set;
import java.util.regex.Pattern;

/** Enforces the Kafka contract again: other producers can bypass HTTP validation. */
public final class TransactionParser {
    private static final ObjectMapper MAPPER = new ObjectMapper(JsonFactory.builder()
            .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION).build())
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    private static final Pattern ID = Pattern.compile("[A-Za-z0-9_-]{1,64}");
    private static final Pattern COUNTRY = Pattern.compile("[A-Z]{2}");
    private static final Set<String> CURRENCIES = Set.of("USD", "CAD", "EUR", "GBP");
    private static final Set<String> CHANNELS = Set.of("WEB", "MOBILE", "POS");
    public static final int MAX_PAYLOAD_CHARACTERS = 16_384;

    private TransactionParser() {}

    public static TransactionEvent parse(String payload, Instant kafkaTimestamp) {
        if (payload == null || payload.isBlank()) throw invalid("empty Kafka value");
        if (payload.length() > MAX_PAYLOAD_CHARACTERS) throw invalid("payload exceeds 16384 characters");
        final JsonNode node;
        try {
            node = MAPPER.readTree(payload);
        } catch (IOException error) {
            // Do not copy untrusted payloads or parser excerpts into logs/errors.
            throw invalid("malformed JSON");
        }
        if (node == null || !node.isObject()) throw invalid("payload must be an object");
        if (integer(node, "schemaVersion") != 1) throw invalid("unsupported schemaVersion");
        String transactionId = identifier(node, "transactionId");
        String accountId = identifier(node, "accountId");
        String merchantId = identifier(node, "merchantId");
        long amount = integer(node, "amountMinor");
        if (amount < 1 || amount > 100_000_000_000L) throw invalid("amountMinor outside allowed range");
        String currency = string(node, "currency");
        if (!CURRENCIES.contains(currency)) throw invalid("unsupported currency");
        String country = string(node, "country");
        if (!COUNTRY.matcher(country).matches()) throw invalid("country must have two uppercase letters");
        String channel = string(node, "channel");
        if (!CHANNELS.contains(channel)) throw invalid("unsupported channel");
        Instant eventTime;
        try {
            eventTime = Instant.parse(string(node, "eventTime"));
        } catch (DateTimeParseException error) {
            throw invalid("eventTime must be an ISO-8601 timestamp with an offset");
        }
        try {
            // Instant accepts a much wider range than Spark's signed microsecond timestamp.
            Math.addExact(Math.multiplyExact(eventTime.getEpochSecond(), 1_000_000L), eventTime.getNano() / 1_000L);
        } catch (ArithmeticException error) {
            throw invalid("eventTime is outside the supported timestamp range");
        }
        // Stable across retries, unlike Instant.now(). A future record must not evict good state.
        if (kafkaTimestamp != null && eventTime.isAfter(kafkaTimestamp.plusSeconds(30))) {
            throw invalid("eventTime is more than 30 seconds ahead of Kafka timestamp");
        }
        return new TransactionEvent(transactionId, accountId, merchantId, amount, currency, country, channel, eventTime);
    }

    private static String identifier(JsonNode node, String field) {
        String value = string(node, field);
        if (!ID.matcher(value).matches()) throw invalid(field + " is not a valid identifier");
        return value;
    }

    private static String string(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || !value.isTextual()) throw invalid(field + " must be a string");
        return value.textValue();
    }

    private static long integer(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || !value.isIntegralNumber() || !value.canConvertToLong()) {
            throw invalid(field + " must be a 64-bit integer");
        }
        return value.longValue();
    }

    private static IllegalArgumentException invalid(String message) {
        return new IllegalArgumentException(message);
    }
}
