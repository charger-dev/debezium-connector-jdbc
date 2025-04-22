/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import static io.debezium.connector.jdbc.JdbcSinkConnectorConfig.InsertMode.CSV;
import static io.debezium.connector.jdbc.JdbcSinkConnectorConfig.InsertMode.INSERT;
import static io.debezium.connector.jdbc.JdbcSinkConnectorConfig.InsertMode.UPSERT;
import static io.debezium.connector.jdbc.JdbcSinkConnectorConfig.SchemaEvolutionMode.NONE;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.hibernate.StatelessSession;
import org.hibernate.Transaction;
import org.hibernate.dialect.DatabaseVersion;
import org.hibernate.query.NativeQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.naming.TableNamingStrategy;
import io.debezium.connector.jdbc.relational.TableDescriptor;
import io.debezium.connector.jdbc.relational.TableId;
import io.debezium.pipeline.sink.spi.ChangeEventSink;
import io.debezium.util.Stopwatch;
import io.debezium.util.Strings;

/**
 * A {@link ChangeEventSink} for a JDBC relational database.
 *
 * @author Chris Cranford
 */
public class JdbcChangeEventSink implements ChangeEventSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcChangeEventSink.class);

    public static final String SCHEMA_CHANGE_VALUE = "SchemaChangeValue";
    public static final String DETECT_SCHEMA_CHANGE_RECORD_MSG = "Schema change records are not supported by JDBC connector. Adjust `topics` or `topics.regex` to exclude schema change topic.";
    private final JdbcSinkConnectorConfig config;
    private final DatabaseDialect dialect;
    private final ConnectionManager connectionManager;
    private final TableNamingStrategy tableNamingStrategy;
    private final RecordWriter recordWriter;

    // Cache for record descriptors to detect schema changes
    private final Map<TableId, SinkRecordDescriptor> previousRecordDescriptors = new ConcurrentHashMap<>();
    private final Map<TableId, TableDescriptor> tableDescriptorCache = new ConcurrentHashMap<>();

    public JdbcChangeEventSink(JdbcSinkConnectorConfig config, ConnectionManager connectionManager, DatabaseDialect dialect, RecordWriter recordWriter) {

        this.config = config;
        this.tableNamingStrategy = config.getTableNamingStrategy();
        this.dialect = dialect;
        this.connectionManager = connectionManager;
        this.recordWriter = recordWriter;
        final DatabaseVersion version = this.dialect.getVersion();
        LOGGER.info("Database version {}.{}.{}", version.getMajor(), version.getMinor(), version.getMicro());
    }

    private StatelessSession openSessionWithRetry() {
        return connectionManager.getStatelessSession();
    }

    @Override
    public void execute(Collection<SinkRecord> records) {

        final Map<TableId, Buffer> updateBufferByTable = new HashMap<>();
        final Map<TableId, Buffer> deleteBufferByTable = new HashMap<>();
        final Map<TableId, Integer> prevColumnCountByTable = new HashMap<>();

        for (SinkRecord record : records) {

            LOGGER.trace("Processing {}", record);

            validate(record);

            Optional<TableId> optionalTableId = getTableId(record);
            if (optionalTableId.isEmpty()) {

                LOGGER.warn("Ignored to write record from topic '{}' partition '{}' offset '{}'. No resolvable table name", record.topic(), record.kafkaPartition(),
                        record.kafkaOffset());
                continue;
            }

            SinkRecordDescriptor sinkRecordDescriptor = buildRecordSinkDescriptor(record);

            final TableId tableId = optionalTableId.get();

            if (sinkRecordDescriptor.isTombstone()) {
                // Skip only Debezium Envelope tombstone not the one produced by ExtractNewRecordState SMT
                LOGGER.debug("Skipping tombstone record {}", sinkRecordDescriptor);
                continue;
            }

            if (sinkRecordDescriptor.isTruncate()) {

                if (!config.isTruncateEnabled()) {
                    LOGGER.debug("Truncates are not enabled, skipping truncate for topic '{}'", sinkRecordDescriptor.getTopicName());
                    continue;
                }

                // Here we want to flush the buffer to let truncate having effect on the buffered events.
                flushBuffers(updateBufferByTable);

                flushBuffers(deleteBufferByTable);

                try {
                    final TableDescriptor table = checkAndApplyTableChangesIfNeeded(tableId, sinkRecordDescriptor, true);
                    writeTruncate(dialect.getTruncateStatement(table));
                }
                catch (SQLException e) {
                    throw new ConnectException("Failed to process a sink record", e);
                }
            }

            if (sinkRecordDescriptor.isDelete()) {

                if (!config.isDeleteEnabled()) {
                    LOGGER.debug("Deletes are not enabled, skipping delete for topic '{}'", sinkRecordDescriptor.getTopicName());
                    continue;
                }

                if (updateBufferByTable.get(tableId) != null && !updateBufferByTable.get(tableId).isEmpty()) {
                    // When an delete arrives, update buffer must be flushed to avoid losing an
                    // delete for the same record after its update.
                    LOGGER.info("Flushing update buffer for table {} because a delete arrived", tableId.getTableName());
                    flushBuffer(tableId, updateBufferByTable.get(tableId).flush(), false);
                }

                Buffer tableIdBuffer = resolveBuffer(deleteBufferByTable, tableId, sinkRecordDescriptor);

                List<SinkRecordDescriptor> toFlush = tableIdBuffer.add(sinkRecordDescriptor);

                flushBuffer(tableId, toFlush, false);
            }
            else {
                if (deleteBufferByTable.get(tableId) != null && !deleteBufferByTable.get(tableId).isEmpty()) {
                    // When an insert arrives, delete buffer must be flushed to avoid losing an insert for the same record after its deletion.
                    // this because at the end we will always flush inserts before deletes.

                    flushBuffer(tableId, deleteBufferByTable.get(tableId).flush(), false);
                }

                if (prevColumnCountByTable.containsKey(tableId) && !Objects.equals(prevColumnCountByTable.get(tableId), sinkRecordDescriptor.getColumnNumber())) {
                    // If the column count is different, we need to flush the buffer to address schema evolution
                    LOGGER.info("Column count changed for table {}. Flushing buffer.",
                            tableId.getTableName() + " " + sinkRecordDescriptor.getColumnNumber() + " " + prevColumnCountByTable.get(tableId));
                    flushBuffer(tableId, updateBufferByTable.get(tableId).flush(), true);
                }

                Stopwatch updateBufferStopwatch = Stopwatch.reusable();
                updateBufferStopwatch.start();

                Buffer tableIdBuffer = resolveBuffer(updateBufferByTable, tableId, sinkRecordDescriptor);

                List<SinkRecordDescriptor> toFlush = tableIdBuffer.add(sinkRecordDescriptor);
                updateBufferStopwatch.stop();

                LOGGER.trace("[PERF] Update buffer execution time {}", updateBufferStopwatch.durations());
                flushBuffer(tableId, toFlush, false);
            }

            prevColumnCountByTable.put(tableId, sinkRecordDescriptor.getColumnNumber());
        }

        flushBuffers(updateBufferByTable);
        flushBuffers(deleteBufferByTable);
    }

    private void validate(SinkRecord record) {

        if (isSchemaChange(record)) {
            LOGGER.error(DETECT_SCHEMA_CHANGE_RECORD_MSG);
            throw new DataException(DETECT_SCHEMA_CHANGE_RECORD_MSG);
        }
    }

    private static boolean isSchemaChange(SinkRecord record) {
        return record.valueSchema() != null
                && !Strings.isNullOrEmpty(record.valueSchema().name())
                && record.valueSchema().name().contains(SCHEMA_CHANGE_VALUE);
    }

    private Buffer resolveBuffer(Map<TableId, Buffer> bufferMap, TableId tableId, SinkRecordDescriptor sinkRecordDescriptor) {
        if (config.isUseReductionBuffer() && !sinkRecordDescriptor.getKeyFieldNames().isEmpty()) {
            return bufferMap.computeIfAbsent(tableId, k -> new ReducedRecordBuffer(config));
        }
        else {
            return bufferMap.computeIfAbsent(tableId, k -> new RecordBuffer(config));
        }
    }

    private SinkRecordDescriptor buildRecordSinkDescriptor(SinkRecord record) {

        SinkRecordDescriptor sinkRecordDescriptor;
        try {
            sinkRecordDescriptor = SinkRecordDescriptor.builder()
                    .withPrimaryKeyMode(config.getPrimaryKeyMode())
                    .withPrimaryKeyFields(config.getPrimaryKeyFields())
                    .withFieldFilters(config.getFieldsFilter())
                    .withSinkRecord(record)
                    .withDialect(dialect)
                    .build();
        }
        catch (Exception e) {
            throw new ConnectException("Failed to process a sink record", e);
        }
        return sinkRecordDescriptor;
    }

    private void flushBuffers(Map<TableId, Buffer> bufferByTable) {

        bufferByTable.forEach((tableId, recordBuffer) -> flushBuffer(tableId, recordBuffer.flush(), false));
    }

    private void flushBuffer(TableId tableId, List<SinkRecordDescriptor> toFlush, boolean schemaChanged) {
        if (toFlush.isEmpty()) {
            return;
        }

        final int maxRetries = 1;
        final long retryDelayMs = 1000;

        Stopwatch flushBufferStopwatch = Stopwatch.reusable();
        Stopwatch tableChangesStopwatch = Stopwatch.reusable();
        int bulkSize = toFlush.size();

        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            try {
                LOGGER.info("Flushing records in JDBC Writer for table {} with size {}{}",
                        tableId.getTableName(), bulkSize,
                        attempt > 0 ? " (retry attempt " + attempt + ")" : "");

                SinkRecordDescriptor record_0 = toFlush.get(0);
                tableChangesStopwatch.start();
                final TableDescriptor table = checkAndApplyTableChangesIfNeeded(tableId, toFlush.get(0), schemaChanged);
                tableChangesStopwatch.stop();

                flushBufferStopwatch.start();

                if (config.getInsertMode() == CSV) {
                    String csvFilePath = "/tmp/" + UUID.randomUUID() + ".csv";
                    List<String> updateStatement = getSqlStatements(table, toFlush.get(toFlush.size() - 1), csvFilePath);
                    try {
                        recordWriter.writeCSV(toFlush, updateStatement, csvFilePath);
                    }
                    finally {
                        try {
                            Files.deleteIfExists(Paths.get(csvFilePath));
                        }
                        catch (IOException e) {
                            LOGGER.error("Failed to delete temporary CSV file {}", csvFilePath, e);
                        }
                    }
                }
                else if (record_0.isDelete()) {
                    String deleteStatement = getSqlStatement(table, toFlush.get(0), true, bulkSize);
                    recordWriter.write(toFlush, deleteStatement, true);
                }
                else if (config.getInsertMode() == INSERT) {
                    String insertStatement = getSqlStatement(table, toFlush.get(0), false, bulkSize);
                    recordWriter.write(toFlush, insertStatement, false);
                }
                else if (config.getInsertMode() == UPSERT) {
                    String deleteStatement = getSqlStatement(table, toFlush.get(0), true, bulkSize);
                    String insertStatement = getSqlStatement(table, toFlush.get(0), false, bulkSize);
                    recordWriter.write(toFlush, deleteStatement, true);
                    recordWriter.write(toFlush, insertStatement, false);
                }
                flushBufferStopwatch.stop();

                LOGGER.trace("[PERF] Flush buffer execution time {}", flushBufferStopwatch.durations());
                LOGGER.trace("[PERF] Table changes execution time {}", tableChangesStopwatch.durations());

                return;
            }
            catch (Exception e) {
                invalidateTableCache(tableId);

                if (attempt >= maxRetries) {
                    throw new ConnectException("Failed to process a sink record after " + (attempt + 1) + " attempts", e);
                }

                LOGGER.warn("Error flushing records for table {}, will retry in {} ms: {}",
                        tableId.getTableName(), retryDelayMs, e.getMessage());

                try {
                    Thread.sleep(retryDelayMs);
                }
                catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new ConnectException("Interrupted while waiting to retry flush operation", ie);
                }
            }
        }
    }

    private Optional<TableId> getTableId(SinkRecord record) {

        String tableName = tableNamingStrategy.resolveTableName(config, record);
        if (tableName == null) {
            return Optional.empty();
        }

        return Optional.of(dialect.getTableId(tableName));
    }

    @Override
    public void close() {
        // Clear caches
        previousRecordDescriptors.clear();
        tableDescriptorCache.clear();
    }

    private void invalidateTableCache(TableId tableId) {
        previousRecordDescriptors.remove(tableId);
        tableDescriptorCache.remove(tableId);
    }

    private TableDescriptor checkAndApplyTableChangesIfNeeded(TableId tableId, SinkRecordDescriptor descriptor, boolean schemaChanged) throws SQLException {
        SinkRecordDescriptor previousDescriptor = previousRecordDescriptors.get(tableId);
        TableDescriptor cachedTableDescriptor = tableDescriptorCache.get(tableId);

        if (!schemaChanged) {
            if (previousDescriptor == null) {
                // First time seeing this table, we need to check/create it
                schemaChanged = true;
                LOGGER.debug("First record for table '{}', checking schema", tableId.toFullIdentiferString());
            }
            else if (cachedTableDescriptor == null) {
                // We have a previous descriptor but no table descriptor, need to check
                schemaChanged = true;
                LOGGER.debug("No cached table descriptor for '{}', checking schema", tableId.toFullIdentiferString());
            }
            else {
                // Check if field count or names have changed
                if (!Objects.equals(descriptor.getColumnNumber(), previousDescriptor.getColumnNumber())) {
                    schemaChanged = true;
                }
                else {
                    // Compare the current descriptor with the previous one
                    Map<String, SinkRecordDescriptor.FieldDescriptor> currentFields = descriptor.getFields();
                    Map<String, SinkRecordDescriptor.FieldDescriptor> previousFields = previousDescriptor.getFields();

                    // Check if any field names or types have changed
                    for (Map.Entry<String, SinkRecordDescriptor.FieldDescriptor> entry : currentFields.entrySet()) {
                        String fieldName = entry.getKey();
                        SinkRecordDescriptor.FieldDescriptor fieldDescriptor = entry.getValue();

                        if (!previousFields.containsKey(fieldName) ||
                                !Objects.equals(previousFields.get(fieldName).getType(), fieldDescriptor.getType()) ||
                                !Objects.equals(previousFields.get(fieldName).getSchema().type(), fieldDescriptor.getSchema().type())) {
                            schemaChanged = true;
                            LOGGER.debug("Field '{}' changed or added in table '{}'",
                                    fieldName, tableId.toFullIdentiferString());
                            break;
                        }
                    }
                }
            }
        }

        // If no schema change detected, return the cached table descriptor
        if (!schemaChanged && cachedTableDescriptor != null) {
            LOGGER.debug("No schema change detected for table '{}', using cached descriptor",
                    tableId.toFullIdentiferString());
            // Update the previous descriptor to the current one
            previousRecordDescriptors.put(tableId, descriptor);
            return cachedTableDescriptor;
        }

        // Schema change detected or first time seeing this table, proceed with normal table check/creation logic
        LOGGER.debug("Schema change detected or first record for table '{}', checking database schema",
                tableId.toFullIdentiferString());

        try {
            createSchema(tableId.getCatalogName());
        }
        catch (SQLException e) {
            LOGGER.info("Schema '{}' already exists.", tableId.getCatalogName());
        }

        TableDescriptor result;
        if (!hasTable(tableId)) {
            // Table does not exist, lets attempt to create it.
            try {
                result = createTable(tableId, descriptor);
            }
            catch (SQLException ce) {
                // It's possible the table may have been created in the interim, so try to alter.
                LOGGER.warn("Table creation failed for '{}', attempting to alter the table", tableId.toFullIdentiferString(), ce);
                try {
                    result = alterTableIfNeeded(tableId, descriptor);
                }
                catch (SQLException ae) {
                    // The alter failed, hard stop.
                    LOGGER.error("Failed to alter the table '{}'.", tableId.toFullIdentiferString(), ae);
                    throw ae;
                }
            }
        }
        else {
            // Table exists, lets attempt to alter it if necessary.
            try {
                result = alterTableIfNeeded(tableId, descriptor);
            }
            catch (SQLException ae) {
                LOGGER.error("Failed to alter the table '{}'.", tableId.toFullIdentiferString(), ae);
                throw ae;
            }
        }

        previousRecordDescriptors.put(tableId, descriptor);
        tableDescriptorCache.put(tableId, result);

        return result;
    }

    private boolean hasTable(TableId tableId) {
        try (StatelessSession session = openSessionWithRetry()) {
            return session.doReturningWork((connection) -> dialect.tableExists(connection, tableId));
        }
    }

    private TableDescriptor readTable(TableId tableId) {
        try (StatelessSession session = openSessionWithRetry()) {
            return session.doReturningWork((connection) -> dialect.readTable(connection, tableId));
        }
    }

    private TableDescriptor createTable(TableId tableId, SinkRecordDescriptor record) throws SQLException {
        LOGGER.debug("Attempting to create table '{}'.", tableId.toFullIdentiferString());

        if (NONE.equals(config.getSchemaEvolutionMode())) {
            LOGGER.warn("Table '{}' cannot be created because schema evolution is disabled.", tableId.toFullIdentiferString());
            throw new SQLException("Cannot create table " + tableId.toFullIdentiferString() + " because schema evolution is disabled");
        }
        try (StatelessSession session = openSessionWithRetry()) {
            Transaction transaction = session.beginTransaction();
            try {
                final String createSql = dialect.getCreateTableStatement(record, tableId);
                LOGGER.trace("SQL: {}", createSql);
                session.createNativeQuery(createSql, Object.class).executeUpdate();
                transaction.commit();
            }
            catch (Exception e) {
                transaction.rollback();
                throw e;
            }
        }

        return readTable(tableId);
    }

    private void createSchema(String schema) throws SQLException {
        LOGGER.debug("Attempting to create schema '{}'.", schema);

        try (StatelessSession session = openSessionWithRetry()) {
            Transaction transaction = session.beginTransaction();
            try {
                final String createSql = dialect.getCreateSchemaStatement(schema);
                session.createNativeQuery(createSql, Object.class).executeUpdate();
                transaction.commit();
            }
            catch (Exception e) {
                transaction.rollback();
                throw e;
            }
        }
    }

    private TableDescriptor alterTableIfNeeded(TableId tableId, SinkRecordDescriptor record) throws SQLException {
        LOGGER.debug("Attempting to alter table '{}'.", tableId.toFullIdentiferString());

        if (!hasTable(tableId)) {
            LOGGER.error("Table '{}' does not exist and cannot be altered.", tableId.toFullIdentiferString());
            throw new SQLException("Could not find table: " + tableId.toFullIdentiferString());
        }

        // Resolve table metadata from the database
        final TableDescriptor table = readTable(tableId);

        // Delegating to dialect to deal with database case sensitivity.
        Set<String> missingFields = dialect.resolveMissingFields(record, table);
        if (missingFields.isEmpty()) {
            // There are no missing fields, simply return
            // todo: should we check column type changes or default value changes?
            return table;
        }

        LOGGER.debug("The follow fields are missing in the table: {}", missingFields);
        if (NONE.equals(config.getSchemaEvolutionMode())) {
            LOGGER.warn("Table '{}' cannot be altered because schema evolution is disabled.", tableId.toFullIdentiferString());
            throw new SQLException("Cannot alter table " + tableId.toFullIdentiferString() + " because schema evolution is disabled");
        }

        try (StatelessSession session = openSessionWithRetry()) {
            Transaction transaction = session.beginTransaction();
            try {
                final String alterSql = dialect.getAlterTableStatement(table, record, missingFields);
                LOGGER.trace("SQL: {}", alterSql);
                session.createNativeQuery(alterSql, Object.class).executeUpdate();
                transaction.commit();
            }
            catch (Exception e) {
                transaction.rollback();
                throw e;
            }
        }

        return readTable(tableId);
    }

    private List<String> getSqlStatements(TableDescriptor table, SinkRecordDescriptor record, String csvFilePath) {
        if (config.getInsertMode() != CSV) {
            throw new DataException("Cannot get SQL statements for non-CSV mode");
        }

        if (record.isDelete()) {
            return dialect.getCSVDeleteStatements(table, record, csvFilePath, record.getKeyFieldNames());
        }
        else {
            return dialect.getCSVUpsertStatements(table, record, csvFilePath, record.getKeyFieldNames(), record.getNonKeyFieldNames());
        }

    }

    private String getSqlStatement(TableDescriptor table, SinkRecordDescriptor record, Boolean isDelete, int bulkSize) {

        if (!record.isDelete() && !isDelete) {
            switch (config.getInsertMode()) {
                case INSERT:
                    return dialect.getInsertStatement(table, record);
                case UPSERT:
                    if (record.getKeyFieldNames().isEmpty()) {
                        throw new ConnectException("Cannot write to table " + table.getId().getTableName() + " with no key fields defined.");
                    }
                    return dialect.getUpsertStatement(table, record);
                case UPDATE:
                    return dialect.getUpdateStatement(table, record);
                default:
                    throw new DataException(String.format("Unknown insert mode: %s", config.getInsertMode()));
            }
        }
        else {
            return dialect.getDeleteStatementBulk(table, record, bulkSize);
        }
    }

    private void writeTruncate(String sql) throws SQLException {
        try (StatelessSession session = openSessionWithRetry()) {
            final Transaction transaction = session.beginTransaction();
            try {
                LOGGER.trace("SQL: {}", sql);
                final NativeQuery<?> query = session.createNativeQuery(sql, Object.class);

                query.executeUpdate();
                transaction.commit();
            }
            catch (Exception e) {
                transaction.rollback();
                throw e;
            }
        }
    }
}
