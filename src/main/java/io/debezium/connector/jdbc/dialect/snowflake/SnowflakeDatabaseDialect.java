/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.snowflake;

import static io.debezium.connector.jdbc.type.debezium.DebeziumZonedTimestampType.NEGATIVE_INFINITY;
import static io.debezium.connector.jdbc.type.debezium.DebeziumZonedTimestampType.POSITIVE_INFINITY;
import static org.hibernate.type.SqlTypes.BLOB;
import static org.hibernate.type.SqlTypes.CLOB;
import static org.hibernate.type.SqlTypes.TIMESTAMP;
import static org.hibernate.type.SqlTypes.TIMESTAMP_WITH_TIMEZONE;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.connect.data.Schema;
import org.hibernate.SessionFactory;
import org.hibernate.boot.model.naming.Identifier;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.jdbc.Size;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.SinkRecordDescriptor;
import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.dialect.DatabaseDialectProvider;
import io.debezium.connector.jdbc.dialect.GeneralDatabaseDialect;
import io.debezium.connector.jdbc.dialect.SqlStatementBuilder;
import io.debezium.connector.jdbc.relational.ColumnDescriptor;
import io.debezium.connector.jdbc.relational.TableDescriptor;
import io.debezium.connector.jdbc.relational.TableId;
import io.debezium.connector.jdbc.type.Type;
import io.debezium.util.Strings;

import net.snowflake.hibernate.dialect.SnowflakeDialect;

/**
 * A {@link DatabaseDialect} implementation for Snowflake.
 *
 * @author Chris Cranford
 */
public class SnowflakeDatabaseDialect extends GeneralDatabaseDialect {

    private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeDatabaseDialect.class);
    private static final String CSV_STAGE_NAME = "DART_STAGE";
    public static final String CSV_NULL_LABEL = "_charger_null";
    private static final String CHARGER_INTERNAL_SCHEMA = "CHARGER_INTERNAL";
    private static final String COLUMN_CHARGER_RAW_ID = "_CHARGER_RAW_ID";
    private static final String COLUMN_CHARGER_EXTRACTED_AT = "_CHARGER_EXTRACTED_AT";
    private static final Map<TableId, DatabaseMetaData> tableMetadataCache = new ConcurrentHashMap<>();

    public static class SnowflakeDatabaseDialectProvider implements DatabaseDialectProvider {
        @Override
        public boolean supports(Dialect dialect) {
            return dialect instanceof SnowflakeDialect;
        }

        @Override
        public Class<?> name() {
            return SnowflakeDatabaseDialect.class;
        }

        @Override
        public DatabaseDialect instantiate(JdbcSinkConnectorConfig config, SessionFactory sessionFactory) {
            return new SnowflakeDatabaseDialect(config, sessionFactory);
        }
    }

    private SnowflakeDatabaseDialect(JdbcSinkConnectorConfig config, SessionFactory sessionFactory) {
        super(config, sessionFactory);
    }

    @Override
    public int getMaxTimestampPrecision() {
        return 6;
    }

    private String getDatabaseName() {
        return getConfig().getConnectionDatabase().toUpperCase();
    }

    private String getSchemaName() {
        return getConfig().getConnectionSchema().toUpperCase();
    }

    private String escapeUnderscores(String input) {
        if (input == null) {
            return null;
        }
        return input.replace("_", "\\_");
    }

    @Override
    public boolean tableExists(Connection connection, TableId tableId) throws SQLException {
        tableId = tableId.toUpperCase();
        final DatabaseMetaData metadata = connection.getMetaData();
        String escapedSchemaName = escapeUnderscores(getSchemaName());

        try (ResultSet rs = metadata.getTables(getDatabaseName(), escapedSchemaName, tableId.getTableName().toUpperCase(), null)) {
            return rs.next();
        }
    }

    @Override
    public TableDescriptor readTable(Connection connection, TableId tableId) throws SQLException {
        tableId = tableId.toUpperCase();
        final TableDescriptor.Builder table = TableDescriptor.builder();
        String escapedSchemaName = escapeUnderscores(getSchemaName());
        String escapedTableName = escapeUnderscores(tableId.getTableName().toUpperCase());

        final DatabaseMetaData metadata = connection.getMetaData();
        try (ResultSet rs = metadata.getTables(getDatabaseName(), escapedSchemaName, tableId.getTableName().toUpperCase(), null)) {
            if (rs.next()) {
                table.catalogName(rs.getString(1));
                table.schemaName(rs.getString(2));
                table.tableName(tableId.getTableName());

                final String tableType = rs.getString(4);
                table.type(Strings.isNullOrBlank(tableType) ? "TABLE" : tableType);
            }
            else {
                throw new IllegalStateException("Failed to find table: " + tableId.toFullIdentiferString());
            }
        }

        try (ResultSet rs = metadata.getColumns(getDatabaseName(), escapedSchemaName, escapedTableName, "%")) {
            while (rs.next()) {
                final String columnName = rs.getString(4);
                final int jdbcType = rs.getInt(5);
                final String typeName = rs.getString(6);
                final int scale = rs.getInt(9);

                final ColumnDescriptor column = ColumnDescriptor.builder()
                        .columnName(columnName)
                        .jdbcType(jdbcType)
                        .typeName(typeName)
                        .scale(scale)
                        .build();

                table.column(column);
            }
        }

        return table.build();
    }

    @Override
    public String getDeleteStatementBulk(TableDescriptor table, SinkRecordDescriptor record, int bulkSize) {
        final SqlStatementBuilder builder = new SqlStatementBuilder();

        List<String> keyFields = record.getKeyFieldNames();
        if (keyFields.isEmpty()) {
            throw new IllegalArgumentException("No key fields defined for delete.");
        }

        String tableAlias = "t";
        String tempAlias = "tmp";

        builder.append("DELETE FROM ");
        builder.append(getQualifiedTableName(table.getId().toUpperCase()));
        builder.append(" ").append(tableAlias).append(" USING (");

        builder.append("SELECT * FROM VALUES ");

        for (int i = 0; i < bulkSize; i++) {
            if (i > 0) {
                builder.append(", ");
            }
            builder.append("(");
            builder.append(String.join(", ", Collections.nCopies(keyFields.size(), "?")));
            builder.append(")");
        }

        builder.append(") AS ").append(tempAlias).append(" (");
        builder.appendList(", ", keyFields, name -> "\"" + name.toUpperCase() + "\"");
        builder.append(") ");

        builder.append("WHERE ");
        for (int i = 0; i < keyFields.size(); i++) {
            if (i > 0) {
                builder.append(" AND ");
            }
            String field = "\"" + keyFields.get(i) + "\"";
            builder.append(tableAlias).append(".").append(field.toUpperCase())
                    .append(" = ").append(tempAlias).append(".").append(field.toUpperCase());
        }

        String sql = builder.build();
        LOGGER.debug("Generated bulk delete statement: {}", sql);
        return sql;
    }

    @Override
    public String getDeleteStatement(TableDescriptor table, SinkRecordDescriptor record) {
        final SqlStatementBuilder builder = new SqlStatementBuilder();
        builder.append("DELETE FROM ");
        builder.append(getQualifiedTableName(table.getId().toUpperCase()));

        if (!record.getKeyFieldNames().isEmpty()) {
            builder.append(" WHERE ");
            builder.appendList(" AND ", record.getKeyFieldNames(), (name) -> columnNameEqualsBinding(name, table, record));
        }

        LOGGER.debug("Generated delete statement: {}", builder.build());
        return builder.build();
    }

    @Override
    public List<String> getCSVDeleteStatements(TableDescriptor table, SinkRecordDescriptor record, String csvFilePath, List<String> keyFieldNames) {
        String dbName = getDatabaseName();
        String tableName = getQualifiedTableName(table.getId());
        String fileName = csvFilePath.substring(csvFilePath.lastIndexOf("/") + 1);
        String tempTableName = "TEMP_" + CHARGER_INTERNAL_SCHEMA + "_" + table.getId().getTableName().toUpperCase() + "_"
                + UUID.randomUUID().toString().replace("-", "_");

        List<String> quotedPkColumns = keyFieldNames.stream()
                .map(col -> "\"" + col.toUpperCase() + "\"")
                .collect(Collectors.toList());

        String joinedPkColumns = String.join(", ", quotedPkColumns);

        String whereClause = quotedPkColumns.stream()
                .map(pk -> String.format("(t1.%s IS NOT DISTINCT FROM t2.%s)", pk, pk))
                .collect(Collectors.joining(" AND "));

        StringBuilder sb = new StringBuilder();

        sb.append("BEGIN;\n");
        sb.append(String.format("CREATE SCHEMA IF NOT EXISTS \"%s\".%s;\n", dbName, CHARGER_INTERNAL_SCHEMA));
        sb.append(String.format("USE SCHEMA \"%s\".%s;\n", dbName, CHARGER_INTERNAL_SCHEMA));
        sb.append(String.format("CREATE STAGE IF NOT EXISTS \"%s\".%s.%s;\n", dbName, CHARGER_INTERNAL_SCHEMA, CSV_STAGE_NAME));
        sb.append(String.format("PUT file://%s @%s PARALLEL = 8;\n", csvFilePath, CSV_STAGE_NAME));
        sb.append(String.format(
                "CREATE OR REPLACE TEMPORARY TABLE \"%s\".%s.\"%s\" LIKE \"%s\".%s;\n",
                dbName, CHARGER_INTERNAL_SCHEMA, tempTableName,
                dbName, tableName));
        sb.append(String.format(
                "COPY INTO \"%s\".%s.\"%s\" (%s) FROM @%s/%s FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '\"' FIELD_DELIMITER = ',' EMPTY_FIELD_AS_NULL = FALSE SKIP_HEADER = 1);\n",
                dbName, CHARGER_INTERNAL_SCHEMA, tempTableName, joinedPkColumns, CSV_STAGE_NAME, fileName));
        sb.append(String.format("REMOVE @%s/%s;\n", CSV_STAGE_NAME, fileName));
        sb.append(String.format(
                "DELETE FROM \"%s\".%s AS t1\n" +
                        "USING \"%s\".%s.\"%s\" AS t2\n" +
                        "WHERE %s;\n",
                dbName, tableName,
                dbName, CHARGER_INTERNAL_SCHEMA, tempTableName,
                whereClause));
        sb.append("COMMIT;\n");

        String[] statements = sb.toString().split(";");
        List<String> sqlStatements = Arrays.stream(statements)
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .map(s -> s + ";")
                .collect(Collectors.toList());

        return sqlStatements;
    }

    @Override
    public String getAlterTablePrefix() {
        return "";
    }

    @Override
    public String getAlterTableSuffix() {
        return "";
    }

    @Override
    public String getAlterTableColumnPrefix() {
        return "ADD COLUMN ";
    }

    @Override
    public String getCreateSchemaStatement(String schema) {
        return "CREATE SCHEMA IF NOT EXISTS \"" + getSchemaName() + "\"";
    }

    @Override
    public String getCreateTableStatement(SinkRecordDescriptor record, TableId tableId) {
        final SqlStatementBuilder builder = new SqlStatementBuilder();
        builder.append("CREATE TABLE ");
        builder.append(getQualifiedTableName(tableId.toUpperCase()));
        builder.append(" (");

        builder.appendLists(", ", record.getKeyFieldNames(), record.getNonKeyFieldNames(), (name) -> {
            final SinkRecordDescriptor.FieldDescriptor field = record.getFields().get(name);
            final String columnName = "\"" + toIdentifier(resolveColumnName(field)) + "\"";
            final String columnType = field.getTypeName();
            return columnName + " " + columnType;
        });

        builder.append(", \"" + COLUMN_CHARGER_EXTRACTED_AT + "\" TIMESTAMP_NTZ");
        builder.append(", \"" + COLUMN_CHARGER_RAW_ID + "\" STRING");
        builder.append(")");

        return builder.build();
    }

    protected String resolveColumnName(SinkRecordDescriptor.FieldDescriptor field) {
        String columnName = columnNamingStrategy.resolveColumnName(field.getColumnName());
        return columnName.toUpperCase();
    }

    @Override
    public String getUpsertStatement(TableDescriptor table, SinkRecordDescriptor record) {
        SqlStatementBuilder builder = new SqlStatementBuilder();

        builder.append("INSERT INTO ");
        builder.append(getQualifiedTableName(table.getId()));
        builder.append(" (");

        builder.appendLists(", ", record.getKeyFieldNames(), record.getNonKeyFieldNames(),
                name -> "\"" + columnNameFromField(name, record) + "\"");

        builder.append(") VALUES (");

        builder.appendLists(", ", record.getKeyFieldNames(), record.getNonKeyFieldNames(),
                name -> columnQueryBindingFromField(name, table, record));

        builder.append(");");

        String sql = builder.build();
        LOGGER.debug("Generated Upsert SQL: {}", sql);
        return sql;
    }

    @Override
    public List<String> getCSVUpsertStatements(TableDescriptor table, SinkRecordDescriptor record, String csvFilePath, List<String> keyFieldNames,
                                               List<String> nonKeyFieldNames, boolean performDeduplication) {
        String dbName = getDatabaseName();
        String tableName = getQualifiedTableName(table.getId());
        String fileName = csvFilePath.substring(csvFilePath.lastIndexOf("/") + 1);

        List<String> quotedColumns = Stream.concat(keyFieldNames.stream(), nonKeyFieldNames.stream())
                .map(col -> "\"" + col.toUpperCase() + "\"")
                .collect(Collectors.toList());
        List<String> quotedPkColumns = keyFieldNames.stream()
                .map(col -> "\"" + col.toUpperCase() + "\"")
                .collect(Collectors.toList());
        String joinedColumns = String.join(", ", quotedColumns);
        String partitionByClause = String.join(", ", quotedPkColumns);

        StringBuilder sb = new StringBuilder();

        sb.append("BEGIN;\n");
        sb.append(String.format("CREATE SCHEMA IF NOT EXISTS \"%s\".%s;\n", dbName, CHARGER_INTERNAL_SCHEMA));
        sb.append(String.format("USE SCHEMA \"%s\".%s;\n", dbName, CHARGER_INTERNAL_SCHEMA));
        sb.append(String.format("CREATE STAGE IF NOT EXISTS \"%s\".%s.%s;\n", dbName, CHARGER_INTERNAL_SCHEMA, CSV_STAGE_NAME));
        sb.append(String.format("PUT file://%s @%s PARALLEL = 8;\n", csvFilePath, CSV_STAGE_NAME));

        sb.append(String.format(
                "COPY INTO \"%s\".%s (%s, %s, %s) FROM @%s/%s FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '\"' FIELD_DELIMITER = ',' EMPTY_FIELD_AS_NULL = FALSE NULL_IF = ('%s') SKIP_HEADER = 1);\n",
                dbName, tableName, joinedColumns, COLUMN_CHARGER_EXTRACTED_AT, COLUMN_CHARGER_RAW_ID, CSV_STAGE_NAME, fileName, CSV_NULL_LABEL));

        sb.append(String.format("REMOVE @%s/%s;\n", CSV_STAGE_NAME, fileName));

        if (performDeduplication) {
            LOGGER.info("Performing deduplication as this is the last batch for topic {}", record.getTopicName());
            sb.append(String.format(
                    "DELETE FROM \"%s\".%s\n" +
                            "WHERE \"%s\" IN (\n" +
                            "    SELECT \"%s\" FROM (\n" +
                            "        SELECT \"%s\",\n" +
                            "               ROW_NUMBER() OVER (PARTITION BY %s\n" +
                            "                                ORDER BY \"%s\" DESC NULLS LAST, \"%s\" DESC) AS row_number\n" +
                            "        FROM \"%s\".%s\n" +
                            "    ) WHERE row_number != 1\n" +
                            ");\n",
                    dbName, tableName,
                    COLUMN_CHARGER_RAW_ID,
                    COLUMN_CHARGER_RAW_ID,
                    COLUMN_CHARGER_RAW_ID,
                    partitionByClause,
                    COLUMN_CHARGER_EXTRACTED_AT, COLUMN_CHARGER_RAW_ID,
                    dbName, tableName));
        }
        else {
            LOGGER.debug("Skipping deduplication for topic {} as performDeduplication={}",
                    record.getTopicName(), performDeduplication);
        }

        sb.append("COMMIT;\n");

        LOGGER.debug("Generated Upsert CSV SQL: {}", sb);
        String[] statements = sb.toString().split(";");
        List<String> sqlStatements = Arrays.stream(statements)
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .map(s -> s + ";")
                .collect(Collectors.toList());

        return sqlStatements;
    }

    @Override
    public String getInsertStatement(TableDescriptor table, SinkRecordDescriptor record) {
        SqlStatementBuilder builder = new SqlStatementBuilder();

        builder.append("INSERT INTO ");
        builder.append(getQualifiedTableName(table.getId()));
        builder.append(" (");

        builder.appendLists(", ", record.getKeyFieldNames(), record.getNonKeyFieldNames(),
                name -> "\"" + columnNameFromField(name, record) + "\"");

        builder.append(") VALUES (");

        builder.appendLists(", ", record.getKeyFieldNames(), record.getNonKeyFieldNames(),
                name -> columnQueryBindingFromField(name, table, record));

        builder.append(");");

        String sql = builder.build();
        LOGGER.debug("Generated Insert SQL: {}", sql);
        return sql;
    }

    @Override
    public String getQueryBindingWithValueCast(ColumnDescriptor column, Schema schema, Type type) {
        if (schema.type() == Schema.Type.STRING) {
            final String typeName = column.getTypeName().toLowerCase();
            if ("uuid".equals(typeName)) {
                return "cast(? as uuid)";
            }
            else if ("json".equals(typeName)) {
                return "cast(? as json)";
            }
            else if ("jsonb".equals(typeName)) {
                return "cast(? as jsonb)";
            }
        }
        return super.getQueryBindingWithValueCast(column, schema, type);
    }

    @Override
    protected String getQualifiedTableName(TableId tableId) {
        if (!Strings.isNullOrBlank(getSchemaName())) {
            return "\"" + getSchemaName() + "\"." + "\"" + tableId.getTableName().toUpperCase() + "\"";
        }
        return "\"" + tableId.getTableName().toUpperCase() + "\"";
    }

    @Override
    public String getByteArrayFormat() {
        return "'0x%s'";
    }

    @Override
    public String getFormattedBoolean(boolean value) {
        // Snowflake maps logical TRUE/FALSE for boolean data types
        return value ? "TRUE" : "FALSE";
    }

    @Override
    public String getFormattedDateTimeWithNanos(TemporalAccessor value) {
        LOGGER.debug("Formatting date time with nanos: {}", value);
        return String.format("'%s'", DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(value));
    }

    @Override
    public String getFormattedTime(TemporalAccessor value) {
        LOGGER.debug("Formatting time: {}", value);
        return String.format("'%s'", DateTimeFormatter.ISO_LOCAL_TIME.format(value));
    }

    @Override
    protected Optional<String> getDatabaseTimeZoneQuery() {
        return Optional.of("SHOW PARAMETERS LIKE 'TIMEZONE';");
    }

    @Override
    protected void registerTypes() {
        super.registerTypes();

        registerType(TextType.INSTANCE);
        registerType(VarcharType.INSTANCE);
        registerType(TimeWithTimezoneType.INSTANCE);
        registerType(TimestampType.INSTANCE);
        registerType(TimestampType2.INSTANCE);
        registerType(CustomConnectStringType.INSTANCE);
    }

    @Override
    public String getTypeName(int jdbcType) {
        switch (jdbcType) {
            case TIMESTAMP_WITH_TIMEZONE:
                return "TIMESTAMP_TZ";
            case TIMESTAMP:
                return "TIMESTAMP_NTZ";
            case CLOB:
                return "VARCHAR(16777216)";
            case BLOB:
                return "BINARY";
            default:
                return ddlTypeRegistry.getTypeName(jdbcType, dialect);
        }
    }

    @Override
    public String getTypeName(int jdbcType, Size size) {
        return getTypeName(jdbcType);
    }

    @Override
    public int getMaxVarcharLengthInKey() {
        return 16777216;
    }

    @Override
    protected String resolveColumnNameFromField(String fieldName) {
        String columnName = super.resolveColumnNameFromField(fieldName);
        if (!getConfig().isQuoteIdentifiers()) {
            // There are specific use cases where we explicitly quote the column name, even if the
            // quoted identifiers is not enabled, such as the Kafka primary key mode column names.
            // If they're quoted, we shouldn't lowercase the column name.
            if (!getIdentifierHelper().toIdentifier(columnName).isQuoted()) {
                // Snowflake defaults to lower case for identifiers
                columnName = columnName.toLowerCase();
            }
        }
        return columnName;
    }

    protected String toIdentifier(TableId tableId) {
        final boolean quoted = getConfig().isQuoteIdentifiers();
        final Identifier catalog = getIdentifierHelper().toIdentifier(getDatabaseName(), quoted);
        final Identifier schema = getIdentifierHelper().toIdentifier(getSchemaName(), quoted);
        final Identifier table = getIdentifierHelper().toIdentifier(tableId.getTableName().toUpperCase(), quoted);

        if (catalog != null && schema != null && table != null) {
            return String.format("%s.%s.%s", catalog.render(dialect), schema.render(dialect), table.render(dialect));
        }
        else if (schema != null && table != null) {
            return String.format("%s.%s", schema.render(dialect), table.render(dialect));
        }
        else if (table != null) {
            return table.render(dialect);
        }
        else {
            throw new IllegalStateException("Expected at least table identifier to be non-null");
        }
    }

    @Override
    public String getTimestampPositiveInfinityValue() {
        return POSITIVE_INFINITY;
    }

    @Override
    public String getTimestampNegativeInfinityValue() {
        return NEGATIVE_INFINITY;
    }

    @Override
    public int getMaxNVarcharLengthInKey() {
        return 16777216;
    }

    @Override
    public String getAlterTableStatement(TableDescriptor table, SinkRecordDescriptor record, Set<String> missingFields) {
        final SqlStatementBuilder builder = new SqlStatementBuilder();
        builder.append("ALTER TABLE ");
        builder.append(getQualifiedTableName(table.getId()));
        builder.append(" ADD ("); // Start ADD clause

        // Append each column definition
        builder.appendList(", ", missingFields, (name) -> {
            final SinkRecordDescriptor.FieldDescriptor field = record.getFields().get(name);
            final StringBuilder columnSpec = new StringBuilder();
            columnSpec.append(toIdentifier(columnNamingStrategy.resolveColumnName(field.getColumnName())));
            columnSpec.append(" ").append(field.getTypeName());
            columnSpec.append(getAlterTableColumnSuffix());
            return columnSpec.toString();
        });

        builder.append(")");
        builder.append(getAlterTableSuffix());
        return builder.build();
    }
}
