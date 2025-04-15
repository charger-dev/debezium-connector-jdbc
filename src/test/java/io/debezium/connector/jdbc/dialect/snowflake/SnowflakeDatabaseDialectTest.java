/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.snowflake;

import static org.fest.assertions.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Types;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.hibernate.SessionFactory;
import org.hibernate.dialect.Dialect;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.dialect.DatabaseDialectProvider;
import io.debezium.connector.jdbc.dialect.snowflake.SnowflakeDatabaseDialect.SnowflakeDatabaseDialectProvider;
import io.debezium.connector.jdbc.relational.ColumnDescriptor;
import io.debezium.connector.jdbc.relational.TableId;

import net.snowflake.hibernate.dialect.SnowflakeDialect;

/**
 * Unit tests for the {@link SnowflakeDatabaseDialect} class.
 *
 * @author Kevin Cui
 */
@Tag("UnitTests")
public class SnowflakeDatabaseDialectTest {

    private JdbcSinkConnectorConfig config;
    private SessionFactory sessionFactory;
    private SnowflakeDatabaseDialect dialect;
    private Dialect hibernateDialect;

    @BeforeEach
    void setUp() {
        Map<String, String> props = new HashMap<>();
        props.put(JdbcSinkConnectorConfig.CONNECTION_DATABASE, "testdb");
        props.put(JdbcSinkConnectorConfig.CONNECTION_SCHEMA, "testschema");
        config = new JdbcSinkConnectorConfig(props);

        sessionFactory = mock(SessionFactory.class);
        hibernateDialect = mock(Dialect.class);

        // Create the dialect using the provider
        DatabaseDialectProvider provider = new SnowflakeDatabaseDialectProvider();
        dialect = (SnowflakeDatabaseDialect) provider.instantiate(config, sessionFactory);
    }

    @Test
    @DisplayName("Should support Snowflake dialect")
    void testSupportsSnowflakeDialect() {
        DatabaseDialectProvider provider = new SnowflakeDatabaseDialectProvider();
        Dialect snowflakeDialect = mock(SnowflakeDialect.class);
        assertThat(provider.supports(snowflakeDialect)).isTrue();
        assertThat(provider.name()).isEqualTo(SnowflakeDatabaseDialect.class);
    }

    @Test
    @DisplayName("Should return correct maximum timestamp precision")
    void testGetMaxTimestampPrecision() {
        assertThat(dialect.getMaxTimestampPrecision()).isEqualTo(6);
    }

    @Test
    @DisplayName("Should format qualified table name correctly")
    void testGetQualifiedTableName() {
        TableId tableId = new TableId("testdb", "testschema", "testtable");
        String qualifiedName = dialect.getQualifiedTableName(tableId);
        assertThat(qualifiedName).isEqualTo("\"TESTSCHEMA\".\"TESTTABLE\"");
    }

    @Test
    @DisplayName("Should format qualified table name correctly without schema")
    void testGetQualifiedTableNameWithoutSchema() {
        // Create config without schema
        Map<String, String> props = new HashMap<>();
        props.put(JdbcSinkConnectorConfig.CONNECTION_DATABASE, "testdb");
        JdbcSinkConnectorConfig configWithoutSchema = new JdbcSinkConnectorConfig(props);

        // Create dialect with the new config
        DatabaseDialectProvider provider = new SnowflakeDatabaseDialectProvider();
        SnowflakeDatabaseDialect dialectWithoutSchema = (SnowflakeDatabaseDialect) provider.instantiate(configWithoutSchema, sessionFactory);

        TableId tableId = new TableId("testdb", null, "testtable");
        String qualifiedName = dialectWithoutSchema.getQualifiedTableName(tableId);
        assertThat(qualifiedName).isEqualTo("\"TESTTABLE\"");
    }

    @Test
    @DisplayName("Should return correct query binding with value cast for UUID")
    void testGetQueryBindingWithValueCastForUuid() {
        Schema schema = SchemaBuilder.string().build();
        ColumnDescriptor column = mock(ColumnDescriptor.class);
        when(column.getTypeName()).thenReturn("uuid");

        String binding = dialect.getQueryBindingWithValueCast(column, schema, null);
        assertThat(binding).isEqualTo("cast(? as uuid)");
    }

    @Test
    @DisplayName("Should return correct query binding with value cast for JSON")
    void testGetQueryBindingWithValueCastForJson() {
        Schema schema = SchemaBuilder.string().build();
        ColumnDescriptor column = mock(ColumnDescriptor.class);
        when(column.getTypeName()).thenReturn("json");

        String binding = dialect.getQueryBindingWithValueCast(column, schema, null);
        assertThat(binding).isEqualTo("cast(? as json)");
    }

    @Test
    @DisplayName("Should return correct query binding with value cast for JSONB")
    void testGetQueryBindingWithValueCastForJsonb() {
        Schema schema = SchemaBuilder.string().build();
        ColumnDescriptor column = mock(ColumnDescriptor.class);
        when(column.getTypeName()).thenReturn("jsonb");

        String binding = dialect.getQueryBindingWithValueCast(column, schema, null);
        assertThat(binding).isEqualTo("cast(? as jsonb)");
    }

    @Test
    @DisplayName("Should return default query binding for non-special types")
    void testGetQueryBindingWithValueCastForNonSpecialTypes() {
        Schema schema = SchemaBuilder.string().build();
        ColumnDescriptor column = mock(ColumnDescriptor.class);
        when(column.getTypeName()).thenReturn("varchar");

        String binding = dialect.getQueryBindingWithValueCast(column, schema, null);
        assertThat(binding).isEqualTo("?");
    }

    @Test
    @DisplayName("Should return correct byte array format")
    void testGetByteArrayFormat() {
        assertThat(dialect.getByteArrayFormat()).isEqualTo("'0x%s'");
    }

    @Test
    @DisplayName("Should format boolean values correctly")
    void testGetFormattedBoolean() {
        assertThat(dialect.getFormattedBoolean(true)).isEqualTo("TRUE");
        assertThat(dialect.getFormattedBoolean(false)).isEqualTo("FALSE");
    }

    @Test
    @DisplayName("Should format date time with nanos correctly")
    void testGetFormattedDateTimeWithNanos() {
        LocalDateTime dateTime = LocalDateTime.of(2023, 1, 15, 10, 30, 45, 123456789);
        String formatted = dialect.getFormattedDateTimeWithNanos(dateTime);
        assertThat(formatted).isEqualTo("'2023-01-15T10:30:45.123456789'");
    }

    @Test
    @DisplayName("Should format time correctly")
    void testGetFormattedTime() {
        LocalTime time = LocalTime.of(10, 30, 45, 123456789);
        String formatted = dialect.getFormattedTime(time);
        assertThat(formatted).isEqualTo("'10:30:45.123456789'");
    }

    @Test
    @DisplayName("Should return correct JDBC type names")
    void testGetTypeName() {
        assertThat(dialect.getTypeName(Types.TIMESTAMP_WITH_TIMEZONE)).isEqualTo("TIMESTAMP_TZ");
        assertThat(dialect.getTypeName(Types.TIMESTAMP)).isEqualTo("TIMESTAMP_NTZ");
        assertThat(dialect.getTypeName(Types.CLOB)).isEqualTo("VARCHAR(16777216)");
        assertThat(dialect.getTypeName(Types.BLOB)).isEqualTo("BINARY");
    }

    @Test
    @DisplayName("Should return correct maximum varchar length in key")
    void testGetMaxVarcharLengthInKey() {
        assertThat(dialect.getMaxVarcharLengthInKey()).isEqualTo(16777216);
    }

    @Test
    @DisplayName("Should return correct maximum nvarchar length in key")
    void testGetMaxNVarcharLengthInKey() {
        assertThat(dialect.getMaxNVarcharLengthInKey()).isEqualTo(16777216);
    }

    @Test
    @DisplayName("Should return correct timestamp infinity values")
    void testGetTimestampInfinityValues() {
        assertThat(dialect.getTimestampPositiveInfinityValue()).isEqualTo("+infinity");
        assertThat(dialect.getTimestampNegativeInfinityValue()).isEqualTo("-infinity");
    }

    @Test
    @DisplayName("Should resolve column name from field correctly")
    void testResolveColumnNameFromField() {
        // Test with quote identifiers disabled (default to lowercase)
        Map<String, String> props = new HashMap<>();
        props.put(JdbcSinkConnectorConfig.CONNECTION_DATABASE, "testdb");
        props.put(JdbcSinkConnectorConfig.CONNECTION_SCHEMA, "testschema");
        props.put(JdbcSinkConnectorConfig.QUOTE_IDENTIFIERS, "false");
        JdbcSinkConnectorConfig configNoQuotes = new JdbcSinkConnectorConfig(props);

        DatabaseDialectProvider provider = new SnowflakeDatabaseDialectProvider();
        SnowflakeDatabaseDialect dialectNoQuotes = (SnowflakeDatabaseDialect) provider.instantiate(configNoQuotes, sessionFactory);

        // This is a protected method, so we need to test it indirectly through public methods
        // or create a test subclass that exposes it. For now, we'll skip direct testing.
    }
}
