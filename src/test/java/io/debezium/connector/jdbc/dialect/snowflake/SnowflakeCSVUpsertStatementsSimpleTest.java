/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.snowflake;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.sink.SinkRecord;
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
import io.debezium.connector.jdbc.SinkRecordDescriptor;
import io.debezium.connector.jdbc.dialect.DatabaseDialectProvider;
import io.debezium.connector.jdbc.relational.TableDescriptor;
import io.debezium.connector.jdbc.relational.TableId;

import net.snowflake.hibernate.dialect.SnowflakeDialect;

/**
 * Unit tests for the {@link SnowflakeDatabaseDialect} CSV upsert statements functionality.
 *
 * @author Kevin Cui
 */
@Tag("UnitTests")
public class SnowflakeCSVUpsertStatementsSimpleTest {

    private SnowflakeDatabaseDialect dialect;
    private JdbcSinkConnectorConfig config;
    private SessionFactory sessionFactory;

    @BeforeEach
    void setUp() {
        // Create a mock config
        Map<String, String> props = new HashMap<>();
        props.put("connection.url", "jdbc:snowflake://account.snowflakecomputing.com");
        props.put("connection.username", "user");
        props.put("connection.password", "password");
        props.put("connection.database", "testdb");
        props.put("connection.schema", "testschema");
        config = new JdbcSinkConnectorConfig(props);

        // Create a properly mocked SessionFactory with all required dependencies
        sessionFactory = mock(SessionFactory.class);
        Dialect hibernateDialect = mock(SnowflakeDialect.class);

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
        DatabaseDialectProvider provider = new SnowflakeDatabaseDialect.SnowflakeDatabaseDialectProvider();
        dialect = (SnowflakeDatabaseDialect) provider.instantiate(config, sessionFactory);
    }

    @Test
    @DisplayName("Should include deduplication query when insertOnly is false")
    void testCSVUpsertStatementsWithDeduplication() {
        // Create mock objects
        TableId tableId = new TableId("testdb", "testschema", "testtable");
        TableDescriptor table = mock(TableDescriptor.class);
        when(table.getId()).thenReturn(tableId);

        SinkRecord sinkRecord = mock(SinkRecord.class);
        when(sinkRecord.topic()).thenReturn("test-topic");
        when(sinkRecord.kafkaPartition()).thenReturn(0);
        when(sinkRecord.kafkaOffset()).thenReturn(100L);

        SinkRecordDescriptor record = mock(SinkRecordDescriptor.class);
        when(record.getTopicName()).thenReturn("test-topic");

        // Call the method with insertOnly = false
        List<String> keyFieldNames = Arrays.asList("id", "name");
        List<String> nonKeyFieldNames = Arrays.asList("value", "description");
        String csvFilePath = "/tmp/test.csv";

        List<String> statements = dialect.getCSVUpsertStatements(table, record, csvFilePath, keyFieldNames, nonKeyFieldNames, true);

        // Verify the statements
        assertThat(statements).isNotNull();
        assertThat(statements).isNotEmpty();

        // Check that the deduplication query is included
        boolean hasDeduplicationQuery = statements.stream()
                .anyMatch(stmt -> stmt.contains("DELETE FROM") && stmt.contains("ROW_NUMBER() OVER"));

        assertThat(hasDeduplicationQuery).isTrue();
    }

    @Test
    @DisplayName("Should not include deduplication query when insertOnly is true")
    void testCSVUpsertStatementsWithoutDeduplication() {
        // Create mock objects
        TableId tableId = new TableId("testdb", "testschema", "testtable");
        TableDescriptor table = mock(TableDescriptor.class);
        when(table.getId()).thenReturn(tableId);

        SinkRecord sinkRecord = mock(SinkRecord.class);
        when(sinkRecord.topic()).thenReturn("test-topic");
        when(sinkRecord.kafkaPartition()).thenReturn(0);
        when(sinkRecord.kafkaOffset()).thenReturn(100L);

        SinkRecordDescriptor record = mock(SinkRecordDescriptor.class);
        when(record.getTopicName()).thenReturn("test-topic");

        // Call the method with insertOnly = true
        List<String> keyFieldNames = Arrays.asList("id", "name");
        List<String> nonKeyFieldNames = Arrays.asList("value", "description");
        String csvFilePath = "/tmp/test.csv";

        List<String> statements = dialect.getCSVUpsertStatements(table, record, csvFilePath, keyFieldNames, nonKeyFieldNames, false);

        // Verify the statements
        assertThat(statements).isNotNull();
        assertThat(statements).isNotEmpty();

        // Check that the deduplication query is NOT included
        boolean hasDeduplicationQuery = statements.stream()
                .anyMatch(stmt -> stmt.contains("DELETE FROM") && stmt.contains("ROW_NUMBER() OVER"));

        assertThat(hasDeduplicationQuery).isFalse();
    }

    @Test
    @DisplayName("Should include all required statements for CSV upsert")
    void testCSVUpsertStatementsContainsRequiredStatements() {
        // Create mock objects
        TableId tableId = new TableId("testdb", "testschema", "testtable");
        TableDescriptor table = mock(TableDescriptor.class);
        when(table.getId()).thenReturn(tableId);

        SinkRecord sinkRecord = mock(SinkRecord.class);
        when(sinkRecord.topic()).thenReturn("test-topic");
        when(sinkRecord.kafkaPartition()).thenReturn(0);
        when(sinkRecord.kafkaOffset()).thenReturn(100L);

        SinkRecordDescriptor record = mock(SinkRecordDescriptor.class);
        when(record.getTopicName()).thenReturn("test-topic");

        // Call the method
        List<String> keyFieldNames = Collections.singletonList("id");
        List<String> nonKeyFieldNames = Collections.singletonList("value");
        String csvFilePath = "/tmp/test.csv";

        List<String> statements = dialect.getCSVUpsertStatements(table, record, csvFilePath, keyFieldNames, nonKeyFieldNames, true);

        // Verify the statements contain all required parts
        assertThat(statements).isNotNull();
        assertThat(statements).isNotEmpty();

        // Check for BEGIN statement
        assertThat(statements.get(0)).contains("BEGIN");

        // Check for CREATE SCHEMA statement
        boolean hasCreateSchema = statements.stream()
                .anyMatch(stmt -> stmt.contains("CREATE SCHEMA IF NOT EXISTS"));
        assertThat(hasCreateSchema).isTrue();

        // Check for CREATE STAGE statement
        boolean hasCreateStage = statements.stream()
                .anyMatch(stmt -> stmt.contains("CREATE STAGE IF NOT EXISTS"));
        assertThat(hasCreateStage).isTrue();

        // Check for PUT statement
        boolean hasPut = statements.stream()
                .anyMatch(stmt -> stmt.contains("PUT file://"));
        assertThat(hasPut).isTrue();

        // Check for COPY INTO statement
        boolean hasCopy = statements.stream()
                .anyMatch(stmt -> stmt.contains("COPY INTO"));
        assertThat(hasCopy).isTrue();

        // Check for REMOVE statement
        boolean hasRemove = statements.stream()
                .anyMatch(stmt -> stmt.contains("REMOVE @"));
        assertThat(hasRemove).isTrue();

        // Check for COMMIT statement
        boolean hasCommit = statements.stream()
                .anyMatch(stmt -> stmt.equals("COMMIT;"));
        assertThat(hasCommit).isTrue();
    }
}
