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

    @Override
    public void bind(ValueBindDescriptor valueBindDescriptor) {

        LOGGER.debug("PreparedStatementQueryBinder Binding value {} to index {}", valueBindDescriptor.getValue(), valueBindDescriptor.getIndex());
        try {
            if (valueBindDescriptor.getTargetSqlType() != null) {
                if (valueBindDescriptor.getTargetSqlType() == Types.ARRAY) {
                    Collection<Object> collection = (Collection<Object>) valueBindDescriptor.getValue();
                    Array array = binder.getConnection().createArrayOf(valueBindDescriptor.getElementTypeName(), collection.toArray());
                    binder.setArray(valueBindDescriptor.getIndex(), array);
                }
                else {
                    Object value = valueBindDescriptor.getValue();

                    if (value instanceof java.time.LocalDateTime) {
                        value = java.sql.Timestamp.valueOf((java.time.LocalDateTime) value);
                    }
                    else if (value instanceof java.time.OffsetDateTime) {
                        value = java.sql.Timestamp.from(((java.time.OffsetDateTime) value).toInstant());
                    }
                    else if (value instanceof java.time.ZonedDateTime) {
                        value = java.sql.Timestamp.from(((java.time.ZonedDateTime) value).toInstant());
                    }
                    else if (value instanceof java.time.Instant) {
                        value = java.sql.Timestamp.from((java.time.Instant) value);
                    }

                    try {
                        binder.setObject(valueBindDescriptor.getIndex(), value, valueBindDescriptor.getTargetSqlType());
                    }
                    catch (SQLException e) {
                        LOGGER.warn("Failed to bind value {} to index {} with targetSqlType {}", valueBindDescriptor.getValue(), valueBindDescriptor.getIndex(),
                                valueBindDescriptor.getTargetSqlType());
                        binder.setObject(valueBindDescriptor.getIndex(), value);
                    }
                }
            }
            else {
                binder.setObject(valueBindDescriptor.getIndex(), valueBindDescriptor.getValue());
            }
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
