/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PreparedStatementQueryBinder implements QueryBinder {

    private final PreparedStatement binder;
    private static final Logger LOGGER = LoggerFactory.getLogger(PreparedStatementQueryBinder.class);

    public PreparedStatementQueryBinder(PreparedStatement binder) {
        this.binder = binder;
    }

    private Object convertValueToSql(Object value) {
        if (value instanceof java.time.LocalDate) {
            return java.sql.Date.valueOf((java.time.LocalDate) value);
        }
        else if (value instanceof java.time.LocalTime) {
            return java.sql.Time.valueOf((java.time.LocalTime) value);
        }
        else if (value instanceof java.time.LocalDateTime) {
            return java.sql.Timestamp.valueOf((java.time.LocalDateTime) value);
        }
        else if (value instanceof java.time.OffsetDateTime) {
            return java.sql.Timestamp.from(((java.time.OffsetDateTime) value).toInstant());
        }
        else if (value instanceof java.time.ZonedDateTime) {
            return java.sql.Timestamp.from(((java.time.ZonedDateTime) value).toInstant());
        }
        else if (value instanceof java.time.Instant) {
            return java.sql.Timestamp.from((java.time.Instant) value);
        }
        else if (value instanceof java.util.Date) {
            LOGGER.debug("Binding java.util.Date");
            return new java.sql.Timestamp(((java.util.Date) value).getTime());
        }
        else if (value instanceof java.util.Calendar) {
            return new java.sql.Timestamp(((java.util.Calendar) value).getTimeInMillis());
        }
        else if (value instanceof java.util.UUID) {
            return value.toString();
        }
        return value;
    }

    @Override
    public void bind(ValueBindDescriptor valueBindDescriptor) {

        LOGGER.debug("PreparedStatementQueryBinder Binding value {} type {} to index {}", valueBindDescriptor.getValue(), valueBindDescriptor.getValue().getClass(),
                valueBindDescriptor.getIndex());
        Object value = convertValueToSql(valueBindDescriptor.getValue());
        try {
            if (valueBindDescriptor.getTargetSqlType() != null) {
                if (valueBindDescriptor.getTargetSqlType() == Types.ARRAY) {
                    Collection<Object> collection = (Collection<Object>) valueBindDescriptor.getValue();
                    Array array = binder.getConnection().createArrayOf(valueBindDescriptor.getElementTypeName(), collection.toArray());
                    binder.setArray(valueBindDescriptor.getIndex(), array);
                }
                else {
                    try {
                        binder.setObject(valueBindDescriptor.getIndex(), value, valueBindDescriptor.getTargetSqlType());
                    }
                    catch (SQLException e) {
                        LOGGER.warn("Failed to bind value {} type {} to index {} with targetSqlType {}", valueBindDescriptor.getValue(),
                                valueBindDescriptor.getValue().getClass(), valueBindDescriptor.getIndex(),
                                valueBindDescriptor.getTargetSqlType());
                        binder.setObject(valueBindDescriptor.getIndex(), value);
                    }
                }
            }
            else {
                binder.setObject(valueBindDescriptor.getIndex(), value);
            }
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
