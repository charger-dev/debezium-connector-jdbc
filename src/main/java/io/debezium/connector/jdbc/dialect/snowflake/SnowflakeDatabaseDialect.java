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
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

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

    @Override
    public boolean tableExists(Connection connection, TableId tableId) throws SQLException {
        tableId = tableId.toUpperCase();
        final DatabaseMetaData metadata = connection.getMetaData();
        try (ResultSet rs = metadata.getTables(getDatabaseName(), getSchemaName(), tableId.getTableName().toUpperCase(), null)) {
            return rs.next();
        }
    }

    @Override
    public TableDescriptor readTable(Connection connection, TableId tableId) throws SQLException {
        tableId = tableId.toUpperCase();
        final TableDescriptor.Builder table = TableDescriptor.builder();

        final DatabaseMetaData metadata = connection.getMetaData();
        try (ResultSet rs = metadata.getTables(getDatabaseName(), getSchemaName(), tableId.getTableName().toUpperCase(), null)) {
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

        try (ResultSet rs = metadata.getColumns(getDatabaseName(), getSchemaName(), tableId.getTableName().toUpperCase(), null)) {
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
        builder.append("DELETE FROM ");
        builder.append(getQualifiedTableName(table.getId().toUpperCase()));

        List<String> keyFields = record.getKeyFieldNames();
        if (!keyFields.isEmpty()) {
            builder.append(" WHERE (");
            builder.appendList(", ", keyFields, name -> columnNameFromField(name, record));
            builder.append(") IN (");

            // Add parameter placeholders for each batch
            for (int i = 0; i < bulkSize; i++) {
                if (i > 0) {
                    builder.append(", ");
                }
                builder.append("(");
                builder.append(String.join(", ", Collections.nCopies(keyFields.size(), "?")));
                builder.append(")");
            }

            builder.append(")");
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
    public String getCreateTableStatement(SinkRecordDescriptor record, TableId tableId) {
        final SqlStatementBuilder builder = new SqlStatementBuilder();
        builder.append("CREATE TABLE ");
        builder.append(getQualifiedTableName(tableId.toUpperCase()));
        builder.append(" (");

        builder.appendLists(", ", record.getKeyFieldNames(), record.getNonKeyFieldNames(), (name) -> {
            final SinkRecordDescriptor.FieldDescriptor field = record.getFields().get(name);
            final String columnName = toIdentifier(resolveColumnName(field));
            final String columnType = field.getTypeName();
            LOGGER.info("Creating column {} with type {}", columnName, columnType);
            return columnName + " " + columnType;
        });
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

        // Append column names (both key and non-key fields)
        builder.appendLists(", ", record.getKeyFieldNames(), record.getNonKeyFieldNames(),
                name -> columnNameFromField(name, record));

        builder.append(") VALUES (");

        // Append corresponding values
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
        builder.append(" ");
        builder.append(getAlterTablePrefix());
        builder.appendList(getAlterTableColumnDelimiter(), missingFields, (name) -> {
            final SinkRecordDescriptor.FieldDescriptor field = record.getFields().get(name);
            final StringBuilder addColumnSpec = new StringBuilder();
            addColumnSpec.append(getAlterTableColumnPrefix());
            addColumnSpec.append(" ");
            addColumnSpec.append(toIdentifier(columnNamingStrategy.resolveColumnName(field.getColumnName())));
            addColumnSpec.append(" ").append(field.getTypeName());
            addColumnSpec.append(getAlterTableColumnSuffix());
            return addColumnSpec.toString();
        });
        builder.append(getAlterTableSuffix());
        return builder.build();
    }
}
