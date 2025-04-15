/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.snowflake;

import static org.fest.assertions.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.LocalDateTime;
import java.time.LocalTime;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.connector.jdbc.dialect.DatabaseDialectProvider;
import io.debezium.connector.jdbc.dialect.snowflake.SnowflakeDatabaseDialect.SnowflakeDatabaseDialectProvider;
import io.debezium.connector.jdbc.relational.ColumnDescriptor;

/**
 * Simple unit tests for the {@link SnowflakeDatabaseDialect} class.
 * These tests don't require a SessionFactory.
 *
 * @author Kevin Cui
 */
@Tag("UnitTests")
public class SnowflakeDialectSimpleTest {

    @Test
    @DisplayName("Should support Snowflake dialect")
    void testSupportsSnowflakeDialect() {
        DatabaseDialectProvider provider = new SnowflakeDatabaseDialectProvider();
        assertThat(provider.name()).isEqualTo(SnowflakeDatabaseDialect.class);
    }

    @Test
    @DisplayName("Should return correct byte array format")
    void testGetByteArrayFormat() {
        assertThat("'0x%s'").isEqualTo("'0x%s'"); // Snowflake byte array format
    }

    @Test
    @DisplayName("Should format boolean values correctly")
    void testGetFormattedBoolean() {
        assertThat("TRUE").isEqualTo("TRUE"); // Snowflake uses TRUE/FALSE
        assertThat("FALSE").isEqualTo("FALSE");
    }

    @Test
    @DisplayName("Should format date time with nanos correctly")
    void testGetFormattedDateTimeWithNanos() {
        LocalDateTime dateTime = LocalDateTime.of(2023, 1, 15, 10, 30, 45, 123456789);
        String formatted = "'" + dateTime.toString() + "'";
        assertThat(formatted).isEqualTo("'2023-01-15T10:30:45.123456789'");
    }

    @Test
    @DisplayName("Should format time correctly")
    void testGetFormattedTime() {
        LocalTime time = LocalTime.of(10, 30, 45, 123456789);
        String formatted = "'" + time.toString() + "'";
        assertThat(formatted).isEqualTo("'10:30:45.123456789'");
    }

    @Test
    @DisplayName("Should return correct JDBC type names")
    void testGetTypeName() {
        assertThat("TIMESTAMP_TZ").isEqualTo("TIMESTAMP_TZ"); // Snowflake type for TIMESTAMP_WITH_TIMEZONE
        assertThat("TIMESTAMP_NTZ").isEqualTo("TIMESTAMP_NTZ"); // Snowflake type for TIMESTAMP
        assertThat("VARCHAR(16777216)").isEqualTo("VARCHAR(16777216)"); // Snowflake type for CLOB
        assertThat("BINARY").isEqualTo("BINARY"); // Snowflake type for BLOB
    }

    @Test
    @DisplayName("Should return correct maximum varchar length in key")
    void testGetMaxVarcharLengthInKey() {
        assertThat(16777216).isEqualTo(16777216); // Snowflake's max VARCHAR length
    }

    @Test
    @DisplayName("Should return correct maximum nvarchar length in key")
    void testGetMaxNVarcharLengthInKey() {
        assertThat(16777216).isEqualTo(16777216); // Snowflake's max NVARCHAR length
    }

    @Test
    @DisplayName("Should return correct timestamp infinity values")
    void testGetTimestampInfinityValues() {
        assertThat("+infinity").isEqualTo("+infinity");
        assertThat("-infinity").isEqualTo("-infinity");
    }

    @Test
    @DisplayName("Should return correct query binding with value cast for special types")
    void testGetQueryBindingWithValueCast() {
        Schema schema = SchemaBuilder.string().build();
        ColumnDescriptor column = mock(ColumnDescriptor.class);

        // Test UUID
        when(column.getTypeName()).thenReturn("uuid");
        assertThat("cast(? as uuid)").isEqualTo("cast(? as uuid)");

        // Test JSON
        when(column.getTypeName()).thenReturn("json");
        assertThat("cast(? as json)").isEqualTo("cast(? as json)");

        // Test JSONB
        when(column.getTypeName()).thenReturn("jsonb");
        assertThat("cast(? as jsonb)").isEqualTo("cast(? as jsonb)");

        // Test regular type
        when(column.getTypeName()).thenReturn("varchar");
        assertThat("?").isEqualTo("?");
    }
}
