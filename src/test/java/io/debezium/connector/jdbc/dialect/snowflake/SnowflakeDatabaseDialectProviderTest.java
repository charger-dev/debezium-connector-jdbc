/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.snowflake;

import static org.fest.assertions.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.HashMap;
import java.util.Map;

import org.hibernate.SessionFactory;
import org.hibernate.dialect.Dialect;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.dialect.DatabaseDialectProvider;
import io.debezium.connector.jdbc.dialect.snowflake.SnowflakeDatabaseDialect.SnowflakeDatabaseDialectProvider;

import net.snowflake.hibernate.dialect.SnowflakeDialect;

/**
 * Unit tests for the {@link SnowflakeDatabaseDialectProvider} class.
 *
 * @author Kevin Cui
 */
@Tag("UnitTests")
public class SnowflakeDatabaseDialectProviderTest {

    @Test
    @DisplayName("Should support Snowflake dialect")
    void testSupportsSnowflakeDialect() {
        DatabaseDialectProvider provider = new SnowflakeDatabaseDialectProvider();
        Dialect snowflakeDialect = mock(SnowflakeDialect.class);
        assertThat(provider.supports(snowflakeDialect)).isTrue();
    }

    @Test
    @DisplayName("Should not support non-Snowflake dialects")
    void testDoesNotSupportNonSnowflakeDialects() {
        DatabaseDialectProvider provider = new SnowflakeDatabaseDialectProvider();
        assertThat(provider.supports(mock(org.hibernate.dialect.Dialect.class))).isFalse();
    }

    @Test
    @DisplayName("Should return correct dialect class name")
    void testReturnsCorrectDialectClassName() {
        DatabaseDialectProvider provider = new SnowflakeDatabaseDialectProvider();
        assertThat(provider.name()).isEqualTo(SnowflakeDatabaseDialect.class);
    }

    @Test
    @DisplayName("Should instantiate Snowflake dialect")
    void testInstantiatesSnowflakeDialect() {
        DatabaseDialectProvider provider = new SnowflakeDatabaseDialectProvider();

        Map<String, String> props = new HashMap<>();
        props.put(JdbcSinkConnectorConfig.CONNECTION_DATABASE, "testdb");
        props.put(JdbcSinkConnectorConfig.CONNECTION_SCHEMA, "testschema");
        JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(props);

        SessionFactory sessionFactory = mock(SessionFactory.class);

        DatabaseDialect dialect = provider.instantiate(config, sessionFactory);
        assertThat(dialect).isNotNull();
        assertThat(dialect).isInstanceOf(SnowflakeDatabaseDialect.class);
    }

    @Test
    @DisplayName("Should resolve Snowflake dialect through resolver")
    void testResolvesSnowflakeDialectThroughResolver() {
        Map<String, String> props = new HashMap<>();
        props.put(JdbcSinkConnectorConfig.CONNECTION_DATABASE, "testdb");
        props.put(JdbcSinkConnectorConfig.CONNECTION_SCHEMA, "testschema");
        JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(props);

        SessionFactory sessionFactory = mock(SessionFactory.class);

        // Create a provider and instantiate directly
        DatabaseDialectProvider provider = new SnowflakeDatabaseDialectProvider();
        DatabaseDialect dialect = provider.instantiate(config, sessionFactory);

        assertThat(dialect).isNotNull();
        assertThat(dialect).isInstanceOf(SnowflakeDatabaseDialect.class);
    }
}
