/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.snowflake;

import static org.fest.assertions.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import org.hibernate.SessionFactory;
import org.hibernate.boot.model.naming.Identifier;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.jdbc.env.spi.IdentifierHelper;
import org.hibernate.engine.jdbc.env.spi.JdbcEnvironment;
import org.hibernate.engine.jdbc.spi.JdbcServices;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.dialect.DatabaseDialectProvider;

import net.snowflake.hibernate.dialect.SnowflakeDialect;

/**
 * Tests for the {@link SnowflakeDatabaseDialect} column name resolution functionality.
 *
 * @author Kevin Cui
 */
@Tag("UnitTests")
public class SnowflakeColumnNameResolutionTest {

    private SnowflakeDatabaseDialect dialect;
    private JdbcSinkConnectorConfig config;
    private SessionFactory sessionFactory;
    private IdentifierHelper identifierHelper;

    @BeforeEach
    void setup() {
        // Set up all required configuration properties
        Map<String, String> props = new HashMap<>();
        props.put(JdbcSinkConnectorConfig.CONNECTION_DATABASE, "testdb");
        props.put(JdbcSinkConnectorConfig.CONNECTION_SCHEMA, "testschema");
        props.put(JdbcSinkConnectorConfig.CONNECTION_URL, "jdbc:snowflake://account.snowflakecomputing.com");
        props.put(JdbcSinkConnectorConfig.CONNECTION_USER, "testuser");
        props.put(JdbcSinkConnectorConfig.CONNECTION_PASSWORD, "testpassword");
        props.put(JdbcSinkConnectorConfig.CONNECTION_POOL_MIN_SIZE, "5");
        props.put(JdbcSinkConnectorConfig.CONNECTION_POOL_MAX_SIZE, "20");

        // Create the config
        config = new JdbcSinkConnectorConfig(props);

        // Mock the session factory and related components
        sessionFactory = mock(SessionFactory.class);
        SessionFactoryImplementor sessionFactoryImplementor = mock(SessionFactoryImplementor.class);
        JdbcServices jdbcServices = mock(JdbcServices.class);
        JdbcEnvironment jdbcEnvironment = mock(JdbcEnvironment.class);
        identifierHelper = mock(IdentifierHelper.class);
        Dialect hibernateDialect = mock(SnowflakeDialect.class);

        // Mock TypeConfiguration to fix NullPointerException
        org.hibernate.type.spi.TypeConfiguration typeConfiguration = mock(org.hibernate.type.spi.TypeConfiguration.class);
        org.hibernate.type.descriptor.jdbc.spi.JdbcTypeRegistry jdbcTypeRegistry = mock(org.hibernate.type.descriptor.jdbc.spi.JdbcTypeRegistry.class);

        when(sessionFactory.unwrap(SessionFactoryImplementor.class)).thenReturn(sessionFactoryImplementor);
        when(sessionFactoryImplementor.getJdbcServices()).thenReturn(jdbcServices);
        when(sessionFactoryImplementor.getTypeConfiguration()).thenReturn(typeConfiguration);
        when(typeConfiguration.getJdbcTypeRegistry()).thenReturn(jdbcTypeRegistry);
        when(jdbcServices.getDialect()).thenReturn(hibernateDialect);
        when(jdbcServices.getJdbcEnvironment()).thenReturn(jdbcEnvironment);
        when(jdbcEnvironment.getIdentifierHelper()).thenReturn(identifierHelper);

        // Create the dialect using the provider
        DatabaseDialectProvider provider = new SnowflakeDatabaseDialect.SnowflakeDatabaseDialectProvider();
        dialect = (SnowflakeDatabaseDialect) provider.instantiate(config, sessionFactory);
    }

    @Test
    @DisplayName("Should resolve regular column names correctly")
    void testResolveRegularColumnNames() throws Exception {
        // We need to use reflection to access the protected method
        Method method = SnowflakeDatabaseDialect.class.getDeclaredMethod("resolveColumnNameFromField", String.class);
        method.setAccessible(true);

        // Mock the identifier helper to return a valid Identifier
        Identifier mockIdentifier = mock(Identifier.class);
        when(mockIdentifier.isQuoted()).thenReturn(false);
        when(identifierHelper.toIdentifier("regular_column")).thenReturn(mockIdentifier);

        // Test with a regular column name
        String result = (String) method.invoke(dialect, "regular_column");
        assertThat(result).isEqualTo("regular_column");
    }

    @Test
    @DisplayName("Should resolve column names with spaces correctly")
    void testResolveColumnNamesWithSpaces() throws Exception {
        // We need to use reflection to access the protected method
        Method method = SnowflakeDatabaseDialect.class.getDeclaredMethod("resolveColumnNameFromField", String.class);
        method.setAccessible(true);

        // Mock the identifier helper to return a valid Identifier
        Identifier mockIdentifier = mock(Identifier.class);
        when(mockIdentifier.isQuoted()).thenReturn(true);
        when(mockIdentifier.getText()).thenReturn("column with spaces");
        when(identifierHelper.toIdentifier("column with spaces")).thenReturn(mockIdentifier);

        // Test with a column name containing spaces
        String result = (String) method.invoke(dialect, "column with spaces");
        assertThat(result).isEqualTo("column with spaces");
    }

    @Test
    @DisplayName("Should resolve column names with special symbols correctly")
    void testResolveColumnNamesWithSpecialSymbols() throws Exception {
        // We need to use reflection to access the protected method
        Method method = SnowflakeDatabaseDialect.class.getDeclaredMethod("resolveColumnNameFromField", String.class);
        method.setAccessible(true);

        // Mock the identifier helper to return a valid Identifier
        Identifier mockIdentifier = mock(Identifier.class);
        when(mockIdentifier.isQuoted()).thenReturn(true);
        when(mockIdentifier.getText()).thenReturn("column#with#symbols");
        when(identifierHelper.toIdentifier("column#with#symbols")).thenReturn(mockIdentifier);

        // Test with a column name containing special symbols
        String result = (String) method.invoke(dialect, "column#with#symbols");
        assertThat(result).isEqualTo("column#with#symbols");
    }

    @Test
    @DisplayName("Should handle quoted identifiers correctly")
    void testResolveQuotedColumnNames() throws Exception {
        // We need to use reflection to access the protected method
        Method method = SnowflakeDatabaseDialect.class.getDeclaredMethod("resolveColumnNameFromField", String.class);
        method.setAccessible(true);

        // Configure the config to use quoted identifiers
        Map<String, String> propsWithQuotes = new HashMap<>();
        propsWithQuotes.put(JdbcSinkConnectorConfig.CONNECTION_DATABASE, "testdb");
        propsWithQuotes.put(JdbcSinkConnectorConfig.CONNECTION_SCHEMA, "testschema");
        propsWithQuotes.put(JdbcSinkConnectorConfig.CONNECTION_URL, "jdbc:snowflake://account.snowflakecomputing.com");
        propsWithQuotes.put(JdbcSinkConnectorConfig.CONNECTION_USER, "testuser");
        propsWithQuotes.put(JdbcSinkConnectorConfig.CONNECTION_PASSWORD, "testpassword");
        propsWithQuotes.put(JdbcSinkConnectorConfig.CONNECTION_POOL_MIN_SIZE, "5");
        propsWithQuotes.put(JdbcSinkConnectorConfig.CONNECTION_POOL_MAX_SIZE, "20");
        propsWithQuotes.put(JdbcSinkConnectorConfig.QUOTE_IDENTIFIERS, "true");

        JdbcSinkConnectorConfig configWithQuotes = new JdbcSinkConnectorConfig(propsWithQuotes);
        DatabaseDialectProvider provider = new SnowflakeDatabaseDialect.SnowflakeDatabaseDialectProvider();
        SnowflakeDatabaseDialect dialectWithQuotes = (SnowflakeDatabaseDialect) provider.instantiate(configWithQuotes, sessionFactory);

        // Mock the identifier helper to return a valid Identifier for quoted column
        Identifier mockIdentifier = mock(Identifier.class);
        when(mockIdentifier.isQuoted()).thenReturn(true);
        when(mockIdentifier.getText()).thenReturn("Quoted Column");
        when(identifierHelper.toIdentifier("\"Quoted Column\"")).thenReturn(mockIdentifier);

        // Test with a quoted column name
        String result = (String) method.invoke(dialectWithQuotes, "\"Quoted Column\"");
        assertThat(result).isEqualTo("\"Quoted Column\"");
    }
}
