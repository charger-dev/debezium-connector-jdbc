/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.snowflake;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.jdbc.ValueBindDescriptor;
import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.type.Type;
import io.debezium.connector.jdbc.type.debezium.ZonedTimeType;
import io.debezium.time.ZonedTime;

/**
 * An implementation of {@link Type} for {@link ZonedTime} types for PostgreSQL.
 *
 * @author Chris Cranford
 */
class TimeWithTimezoneType extends ZonedTimeType {

    public static final TimeWithTimezoneType INSTANCE = new TimeWithTimezoneType();
    private static final Logger LOGGER = LoggerFactory.getLogger(TimeWithTimezoneType.class);

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ ZonedTime.SCHEMA_NAME };
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {
        LOGGER.debug("TEST TimeWithTimezone bind");
        if (value == null) {
            return List.of(new ValueBindDescriptor(index, null));
        }

        try {
            final ZonedDateTime zdt = OffsetTime.parse((String) value, ZonedTime.FORMATTER)
                    .atDate(LocalDate.now())
                    .toZonedDateTime();

            ZonedDateTime adjusted = zdt;
            if (getDialect().isTimeZoneSet()) {
                if (getDialect().shouldBindTimeWithTimeZoneAsDatabaseTimeZone()) {
                    adjusted = zdt.withZoneSameInstant(getDatabaseTimeZone().toZoneId());
                }
            }

            Timestamp timestamp = Timestamp.from(adjusted.toInstant());
            return List.of(new ValueBindDescriptor(index, timestamp));
        }
        catch (Exception e) {
            throw new ConnectException("Failed to parse timestamp value: " + value, e);
        }
    }

    public String getTypeName(DatabaseDialect dialect, Schema schema, boolean key) {
        return "TIMESTAMP_NTZ";
    }
}
