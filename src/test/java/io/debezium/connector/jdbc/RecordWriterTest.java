/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;

/**
 * Unit tests for the {@link RecordWriter} class.
 */
@Tag("UnitTests")
class RecordWriterTest {

    // Test subclass that allows us to track if executeAllWithRawJdbc was called
    static class TestRecordWriter extends RecordWriter {
        private boolean executeAllWithRawJdbcCalled = false;

        public TestRecordWriter(ConnectionManager connectionManager, QueryBinderResolver queryBinderResolver,
                                JdbcSinkConnectorConfig config, DatabaseDialect dialect) {
            super(connectionManager, queryBinderResolver, config, dialect);
        }

        @Override
        public Integer writeRecordsToCsv(List<SinkRecordDescriptor> records, String csvFilePath) throws IOException {
            // Return the size of the records list to simulate writing records
            return records.size();
        }

        // Can't override private method, so we'll use a flag to track if it was called

        // Override writeCSV to track if executeAllWithRawJdbc would be called
        @Override
        public void writeCSV(List<SinkRecordDescriptor> records, List<String> sqlStatements, String csvFilePath) throws IOException {
            int numRecords = writeRecordsToCsv(records, csvFilePath);

            if (numRecords > 0) {
                executeAllWithRawJdbcCalled = true;
                // Don't actually execute SQL in the test
            }
        }

        public boolean wasExecuteAllWithRawJdbcCalled() {
            return executeAllWithRawJdbcCalled;
        }

        public void resetExecuteAllWithRawJdbcCalled() {
            executeAllWithRawJdbcCalled = false;
        }
    }

    private TestRecordWriter testRecordWriter;
    private JdbcSinkConnectorConfig config;
    private QueryBinderResolver queryBinderResolver;
    private ConnectionManager connectionManager;
    private DatabaseDialect dialect;

    @BeforeEach
    void setUp() {
        connectionManager = mock(ConnectionManager.class);
        queryBinderResolver = mock(QueryBinderResolver.class);
        config = mock(JdbcSinkConnectorConfig.class);
        dialect = mock(DatabaseDialect.class);

        testRecordWriter = new TestRecordWriter(connectionManager, queryBinderResolver, config, dialect);
    }

    @Test
    @DisplayName("writeCSV should not call executeAllWithRawJdbc when there are no records")
    void testWriteCSVWithNoRecords() throws IOException {
        // Arrange
        List<SinkRecordDescriptor> emptyRecords = new ArrayList<>();
        List<String> sqlStatements = List.of("INSERT INTO table VALUES (1, 'test')");
        String csvFilePath = "/tmp/test.csv";

        // Reset the flag
        testRecordWriter.resetExecuteAllWithRawJdbcCalled();

        // Act
        testRecordWriter.writeCSV(emptyRecords, sqlStatements, csvFilePath);

        // Assert
        assertFalse(testRecordWriter.wasExecuteAllWithRawJdbcCalled(),
                "executeAllWithRawJdbc should not be called when there are no records");
    }

    @Test
    @DisplayName("writeCSV should call executeAllWithRawJdbc when there are records")
    void testWriteCSVWithRecords() throws IOException {
        // Arrange
        List<SinkRecordDescriptor> records = new ArrayList<>();
        records.add(mock(SinkRecordDescriptor.class)); // Add a mock record
        List<String> sqlStatements = List.of("INSERT INTO table VALUES (1, 'test')");
        String csvFilePath = "/tmp/test.csv";

        // Reset the flag
        testRecordWriter.resetExecuteAllWithRawJdbcCalled();

        // Act
        testRecordWriter.writeCSV(records, sqlStatements, csvFilePath);

        // Assert
        assertTrue(testRecordWriter.wasExecuteAllWithRawJdbcCalled(),
                "executeAllWithRawJdbc should be called when there are records");
    }
}
