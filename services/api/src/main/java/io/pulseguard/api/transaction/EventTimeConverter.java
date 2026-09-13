package io.pulseguard.api.transaction;

import java.time.Instant;
import org.springframework.data.mongodb.core.convert.MongoConversionContext;
import org.springframework.data.mongodb.core.convert.MongoValueConverter;

/** BSON dates truncate nanoseconds; preserve exact event time for payload equality on replay. */
public class EventTimeConverter implements MongoValueConverter<Instant, String> {
    @Override
    public Instant read(String value, MongoConversionContext context) { return Instant.parse(value); }

    @Override
    public String write(Instant value, MongoConversionContext context) { return value.toString(); }
}
