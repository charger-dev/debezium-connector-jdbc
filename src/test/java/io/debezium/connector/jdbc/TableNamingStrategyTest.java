/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import static org.fest.assertions.Assertions.assertThat;

import java.util.Map;

import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.connector.jdbc.naming.DefaultTableNamingStrategy;
import io.debezium.connector.jdbc.naming.TableNamingStrategy;
import io.debezium.connector.jdbc.util.DebeziumSinkRecordFactory;
import io.debezium.connector.jdbc.util.SinkRecordFactory;
import io.debezium.doc.FixFor;

/**
 * Tests for the {@link TableNamingStrategy} interface and implementations.
 *
 * @author Chris Cranford
 */
@Tag("UnitTests")
public class TableNamingStrategyTest {
    @Test
    public void testDefaultTableNamingStrategy() {
        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(Map.of());
        final SinkRecordFactory factory = new DebeziumSinkRecordFactory();
        final DefaultTableNamingStrategy strategy = new DefaultTableNamingStrategy();
        assertThat(strategy.resolveTableName(config, factory.createRecord("database.schema.table"))).isEqualTo("TABLE");
    }

    @Test
    public void testTableNamingStrategyWithTableNameFormat() {
        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(Map.of("table.name.format", "kafka_${topic}"));
        final SinkRecordFactory factory = new DebeziumSinkRecordFactory();
        final DefaultTableNamingStrategy strategy = new DefaultTableNamingStrategy();
        assertThat(strategy.resolveTableName(config, factory.createRecord("database.schema.table"))).isEqualTo("TABLE");
    }

    @Test
    @FixFor("DBZ-6491")
    public void testTableNamingStrategyWithPrependedSchema() {
        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(Map.of("table.name.format", "SYS.${topic}"));
        final SinkRecordFactory factory = new DebeziumSinkRecordFactory();
        final DefaultTableNamingStrategy strategy = new DefaultTableNamingStrategy();
        assertThat(strategy.resolveTableName(config, factory.createRecord("database.schema.table"))).isEqualTo("TABLE");
    }

    @Test
    public void testDefaultTableNamingStrategyWithDebeziumSource() {
        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(Map.of("table.name.format", "source_${source.db}_${source.schema}_${source.table}"));
        final SinkRecordFactory factory = new DebeziumSinkRecordFactory();
        final DefaultTableNamingStrategy strategy = new DefaultTableNamingStrategy();
        SinkRecord sinkRecord = factory.createRecord("database.schema.table", (byte) 1, "database1", "schema1", "table1");
        assertThat(strategy.resolveTableName(config, sinkRecord)).isEqualTo("TABLE");
    }

    @Test
    public void testDefaultTableNamingStrategyWithInvalidSourceField() {
        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(Map.of("table.name.format", "source_${source.invalid}"));
        final SinkRecordFactory factory = new DebeziumSinkRecordFactory();
        final DefaultTableNamingStrategy strategy = new DefaultTableNamingStrategy();
        SinkRecord sinkRecord = factory.createRecord("database.schema.table", (byte) 1, "database1", "schema1", "table1");
        assertThat(strategy.resolveTableName(config, sinkRecord)).isEqualTo("TABLE");
    }

    @Test
    public void testDefaultTableNamingStrategyWithTopicAndTombstone() {
        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(Map.of("table.name.format", "kafka_${topic}"));
        final SinkRecordFactory factory = new DebeziumSinkRecordFactory();
        final DefaultTableNamingStrategy strategy = new DefaultTableNamingStrategy();
        SinkRecord sinkRecord = factory.tombstoneRecord("database.schema.table");
        assertThat(strategy.resolveTableName(config, sinkRecord)).isEqualTo("TABLE");
    }
}
