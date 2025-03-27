/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.snowflake;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.type.AbstractType;
import io.debezium.connector.jdbc.type.Type;
import io.debezium.data.Uuid;

/**
 * An implementation of {@link Type} for {@link Uuid} types.
 *
 * @author Chris Cranford
 */
class VarcharType extends AbstractType {

    public static final VarcharType INSTANCE = new VarcharType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ "VARCHAR" };
    }

    @Override
    public String getTypeName(DatabaseDialect dialect, Schema schema, boolean key) {
        return "varchar";
    }

}
