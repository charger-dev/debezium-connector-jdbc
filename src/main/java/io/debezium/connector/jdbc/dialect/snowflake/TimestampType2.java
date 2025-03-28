/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.snowflake;

import java.time.Instant;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.jdbc.ValueBindDescriptor;
import io.debezium.connector.jdbc.relational.ColumnDescriptor;
import io.debezium.connector.jdbc.type.AbstractTimestampType;

public class TimestampType2 extends AbstractTimestampType {

    private static final Logger LOGGER = LoggerFactory.getLogger(TimestampType2.class);
    public static final TimestampType2 INSTANCE = new TimestampType2();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ Timestamp.LOGICAL_NAME };
    }

    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {
        LOGGER.debug("TEST Timestamp2 bind");
        if (value == null) {
            return List.of(new ValueBindDescriptor(index, null));
        }

        // Handle Debezium io.debezium.time.Timestamp (epoch millis)
        if (!(value instanceof Long)) {
            throw new ConnectException("Expected Long for io.debezium.time.Timestamp, but got: " + value.getClass());
        }

        long epochMillis = (Long) value;
        java.sql.Timestamp timestamp = java.sql.Timestamp.from(Instant.ofEpochMilli(epochMillis));
        return List.of(new ValueBindDescriptor(index, timestamp));
    }

    @Override
    public String getQueryBinding(ColumnDescriptor column, Schema schema, Object value) {
        return getDialect().getTimeQueryBinding();
    }
}
