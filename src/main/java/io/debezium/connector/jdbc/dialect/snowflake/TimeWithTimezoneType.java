/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.snowflake;

import org.apache.kafka.connect.data.Schema;

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

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ ZonedTime.SCHEMA_NAME };
    }

    public String getTypeName(DatabaseDialect dialect, Schema schema, boolean key) {
        return "TIMESTAMP_NTZ";
    }
}
