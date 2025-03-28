/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.type.connect;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Date;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.errors.ConnectException;

import io.debezium.connector.jdbc.ValueBindDescriptor;
import io.debezium.connector.jdbc.relational.ColumnDescriptor;
import io.debezium.connector.jdbc.type.AbstractTimeType;
import io.debezium.connector.jdbc.type.Type;
import io.debezium.connector.jdbc.util.DateTimeUtils;

/**
 * An implementation of {@link Type} for {@link org.apache.kafka.connect.data.Date} values.
 *
 * @author Chris Cranford
 */
public class ConnectTimeType extends AbstractTimeType {

    public static final ConnectTimeType INSTANCE = new ConnectTimeType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ Time.LOGICAL_NAME };
    }

    @Override
    public String getQueryBinding(ColumnDescriptor column, Schema schema, Object value) {
        return getDialect().getTimeQueryBinding();
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {

        if (value == null) {
            return List.of(new ValueBindDescriptor(index, null));
        }
        if (value instanceof Date) {

            final LocalTime localTime = DateTimeUtils.toLocalTimeFromUtcDate((Date) value);
            final LocalDateTime localDateTime = localTime.atDate(LocalDate.now());
            if (getDialect().isTimeZoneSet()) {
                return List.of(new ValueBindDescriptor(index, localDateTime.atZone(getDatabaseTimeZone().toZoneId()).toLocalDateTime().toLocalTime()));
            }

            return List.of(new ValueBindDescriptor(index, localDateTime.toLocalTime()));
        }

        throw new ConnectException(String.format("Unexpected %s value '%s' with type '%s'", getClass().getSimpleName(),
                value, value.getClass().getName()));
    }

}
