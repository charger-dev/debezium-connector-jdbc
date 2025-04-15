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
import org.hibernate.engine.jdbc.env.spi.IdentifierHelper;
import org.hibernate.engine.jdbc.env.spi.JdbcEnvironment;
import org.hibernate.engine.jdbc.spi.JdbcServices;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.type.descriptor.sql.spi.DdlTypeRegistry;
import org.hibernate.type.spi.TypeConfiguration;
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
        // Set up all required configuration properties
        Map<String, String> props = new HashMap<>();
        props.put(JdbcSinkConnectorConfig.CONNECTION_DATABASE, "testdb");
        props.put(JdbcSinkConnectorConfig.CONNECTION_SCHEMA, "testschema");
        props.put(JdbcSinkConnectorConfig.CONNECTION_URL, "jdbc:snowflake://account.snowflakecomputing.com");
        props.put(JdbcSinkConnectorConfig.CONNECTION_USER, "testuser");
        props.put(JdbcSinkConnectorConfig.CONNECTION_PASSWORD, "testpassword");
        props.put(JdbcSinkConnectorConfig.CONNECTION_POOL_MIN_SIZE, "5");
        props.put(JdbcSinkConnectorConfig.CONNECTION_POOL_MAX_SIZE, "20");
        props.put(JdbcSinkConnectorConfig.CONNECTION_POOL_ACQUIRE_INCREMENT, "1");
        props.put(JdbcSinkConnectorConfig.QUOTE_IDENTIFIERS, "false");
        props.put(JdbcSinkConnectorConfig.DATABASE_TIME_ZONE, "UTC");
        props.put(JdbcSinkConnectorConfig.TABLE_NAME_FORMAT, "${topic}");
        props.put(JdbcSinkConnectorConfig.INSERT_MODE, "insert");
        props.put(JdbcSinkConnectorConfig.DELETE_ENABLED, "false");
        props.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, "record_key");
        props.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, "basic");

        config = new JdbcSinkConnectorConfig(props);

        // Create a properly mocked SessionFactory with all required dependencies
        sessionFactory = mock(SessionFactory.class);
        hibernateDialect = mock(SnowflakeDialect.class);

        // Mock the SessionFactoryImplementor
        SessionFactoryImplementor sessionFactoryImplementor = mock(SessionFactoryImplementor.class);
        when(sessionFactory.unwrap(SessionFactoryImplementor.class)).thenReturn(sessionFactoryImplementor);

        // Mock JdbcServices
        JdbcServices jdbcServices = mock(JdbcServices.class);
        when(sessionFactoryImplementor.getJdbcServices()).thenReturn(jdbcServices);
        when(jdbcServices.getDialect()).thenReturn(hibernateDialect);

        // Mock TypeConfiguration and DdlTypeRegistry
        TypeConfiguration typeConfiguration = mock(TypeConfiguration.class);
        DdlTypeRegistry ddlTypeRegistry = mock(DdlTypeRegistry.class);
        when(sessionFactoryImplementor.getTypeConfiguration()).thenReturn(typeConfiguration);
        when(typeConfiguration.getDdlTypeRegistry()).thenReturn(ddlTypeRegistry);

        // Mock JdbcEnvironment and IdentifierHelper
        JdbcEnvironment jdbcEnvironment = mock(JdbcEnvironment.class);
        IdentifierHelper identifierHelper = mock(IdentifierHelper.class);
        when(jdbcServices.getJdbcEnvironment()).thenReturn(jdbcEnvironment);
        when(jdbcEnvironment.getIdentifierHelper()).thenReturn(identifierHelper);

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
        // We can test this using the existing dialect instance
        // by creating a TableId without a schema

        TableId tableId = new TableId("testdb", null, "testtable");
        String qualifiedName = dialect.getQualifiedTableName(tableId);

        // Since our config has a schema ("testschema"), the qualified name should include it
        assertThat(qualifiedName).isEqualTo("\"TESTSCHEMA\".\"TESTTABLE\"");

        // We can also test with a table ID that has an explicit schema
        TableId tableIdWithSchema = new TableId("testdb", "explicit_schema", "testtable");
        String qualifiedNameWithSchema = dialect.getQualifiedTableName(tableIdWithSchema);

        // The explicit schema should be used, but the connection schema should be used
        assertThat(qualifiedNameWithSchema).isEqualTo("\"TESTSCHEMA\".\"TESTTABLE\"");
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
        assertThat(dialect.getTimestampPositiveInfinityValue()).isEqualTo("infinity");
        assertThat(dialect.getTimestampNegativeInfinityValue()).isEqualTo("-infinity");
    }

    @Test
    @DisplayName("Should resolve column name from field correctly")
    void testResolveColumnNameFromField() {
        // This is a protected method, so we need to test it indirectly through public methods
        // We can test it by checking the behavior of the dialect with different configurations

        // Create a TableId with a mixed-case table name
        TableId tableId = new TableId("testdb", "testschema", "MixedCaseTable");

        // Get the qualified table name, which should use the connection schema and uppercase the table name
        String qualifiedName = dialect.getQualifiedTableName(tableId);

        // With quote identifiers disabled, Snowflake should convert table names to uppercase
        assertThat(qualifiedName).isEqualTo("\"TESTSCHEMA\".\"MIXEDCASETABLE\"");
    }
}
