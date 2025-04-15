/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.snowflake;

import static org.fest.assertions.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.hibernate.SessionFactory;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.jdbc.env.spi.IdentifierHelper;
import org.hibernate.engine.jdbc.env.spi.JdbcEnvironment;
import org.hibernate.engine.jdbc.spi.JdbcServices;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.type.descriptor.sql.spi.DdlTypeRegistry;
import org.hibernate.type.spi.TypeConfiguration;
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

        JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(props);

        // Create a properly mocked SessionFactory with all required dependencies
        SessionFactory sessionFactory = mock(SessionFactory.class);
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

        DatabaseDialect dialect = provider.instantiate(config, sessionFactory);
        assertThat(dialect).isNotNull();
        assertThat(dialect).isInstanceOf(SnowflakeDatabaseDialect.class);
    }

    @Test
    @DisplayName("Should resolve Snowflake dialect through resolver")
    void testResolvesSnowflakeDialectThroughResolver() {
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

        JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(props);

        // Create a properly mocked SessionFactory with all required dependencies
        SessionFactory sessionFactory = mock(SessionFactory.class);
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

        // Create a provider and instantiate directly
        DatabaseDialectProvider provider = new SnowflakeDatabaseDialectProvider();
        DatabaseDialect dialect = provider.instantiate(config, sessionFactory);

        assertThat(dialect).isNotNull();
        assertThat(dialect).isInstanceOf(SnowflakeDatabaseDialect.class);
    }
}
