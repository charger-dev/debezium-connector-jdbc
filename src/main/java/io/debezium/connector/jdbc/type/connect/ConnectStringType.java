/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.type.connect;

import java.sql.Types;
import java.util.Optional;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.type.Type;
import io.debezium.util.Strings;

/**
 * An implementation of {@link Type} that supports {@code STRING} connect schema types.
 *
 * @author Chris Cranford
 */
public class ConnectStringType extends AbstractConnectSchemaType {

    public static final ConnectStringType INSTANCE = new ConnectStringType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ "STRING" };
    }

    @Override
    public String getTypeName(DatabaseDialect dialect, Schema schema, boolean key) {
        final int jdbcType = hasNationalizedCharacterSet(schema) ? Types.NVARCHAR : Types.VARCHAR;
        return dialect.getTypeName(jdbcType);
    }

    private int getColumnSize(DatabaseDialect dialect, Schema schema, boolean key, int jdbcType) {
        int columnSize = Integer.parseInt(getSourceColumnSize(schema).orElse("0"));
        if (key) {
            final int maxSizeInKey = getMaxSizeInKey(dialect, jdbcType);
            if (columnSize > 0) {
                columnSize = Math.min(columnSize, maxSizeInKey);
            }
            else {
                columnSize = maxSizeInKey;
            }
        }
        return columnSize;
    }

    private int getMaxSizeInKey(DatabaseDialect dialect, int jdbcType) {
        if (jdbcType == Types.NCHAR || jdbcType == Types.NVARCHAR) {
            return dialect.getMaxNVarcharLengthInKey();
        }
        return dialect.getMaxVarcharLengthInKey();
    }

    private int getColumnSqlType(Schema schema) {
        final Optional<String> columnType = getSourceColumnType(schema);
        if (columnType.isPresent()) {
            final String type = columnType.get();
            // PostgreSQL represents characters as BPCHAR data types
            if (isType(type, "CHAR", "CHARACTER", "BPCHAR")) {
                return hasNationalizedCharacterSet(schema) ? Types.NCHAR : Types.CHAR;
            }
            else if (isType(type, "NCHAR")) {
                return Types.NCHAR;
            }
            else if (isType(type, "VARCHAR", "VARCHAR2", "CHARACTER VARYING")) {
                return hasNationalizedCharacterSet(schema) ? Types.NVARCHAR : Types.VARCHAR;
            }
            else if (isType(type, "NVARCHAR", "NVARCHAR2")) {
                return Types.NVARCHAR;
            }
        }
        return Types.OTHER;
    }

    private static boolean isType(String columnType, String... possibilities) {
        for (String possibility : possibilities) {
            if (possibility.equalsIgnoreCase(columnType)) {
                return true;
            }
        }
        return false;
    }

    private boolean hasNationalizedCharacterSet(Schema schema) {
        // MySQL emits a __debezium.source.column.character_set property to pass the character set name
        // downstream in the pipeline, which is useful for the sink connector to resolve whether the
        // column should be mapped to a nationalized variant (NCHAR/NVARCHAR)
        if (schema.parameters() != null) {
            final String charsetName = schema.parameters().get("__debezium.source.column.character_set");
            return !Strings.isNullOrEmpty(charsetName) && charsetName.toLowerCase().startsWith("utf8");
        }
        return false;
    }
}
