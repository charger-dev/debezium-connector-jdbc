/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.snowflake;

import static org.fest.assertions.Assertions.assertThat;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.service.ServiceRegistry;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.dialect.DatabaseDialectProvider;
import io.debezium.connector.jdbc.relational.TableId;

/**
 * Integration tests for the {@link SnowflakeDatabaseDialect} class.
 * These tests require a Snowflake connection and will be skipped if the environment variables are not set.
 *
 * @author Kevin Cui
 */
@Tag("IntegrationTests")
@EnabledIfEnvironmentVariable(named = "SNOWFLAKE_URL", matches = ".*")
public class SnowflakeDatabaseDialectIT {

    private static final String SNOWFLAKE_URL = System.getenv("SNOWFLAKE_URL");
    private static final String SNOWFLAKE_USER = System.getenv("SNOWFLAKE_USER");
    private static final String SNOWFLAKE_PASSWORD = System.getenv("SNOWFLAKE_PASSWORD");
    private static final String SNOWFLAKE_DATABASE = System.getenv("SNOWFLAKE_DATABASE");
    private static final String SNOWFLAKE_SCHEMA = System.getenv("SNOWFLAKE_SCHEMA");

    private static SessionFactory sessionFactory;
    private static DatabaseDialect dialect;
    private static Connection connection;

    @BeforeAll
    static void setUp() throws SQLException {
        // Create Hibernate configuration
        Configuration hibernateConfig = new Configuration();
        hibernateConfig.setProperty("hibernate.connection.url", SNOWFLAKE_URL);
        hibernateConfig.setProperty("hibernate.connection.username", SNOWFLAKE_USER);
        hibernateConfig.setProperty("hibernate.connection.password", SNOWFLAKE_PASSWORD);
        hibernateConfig.setProperty("hibernate.connection.driver_class", "net.snowflake.client.jdbc.SnowflakeDriver");
        hibernateConfig.setProperty("hibernate.dialect", "org.hibernate.dialect.SnowflakeDialect");

        // Create service registry and session factory
        ServiceRegistry serviceRegistry = new StandardServiceRegistryBuilder()
                .applySettings(hibernateConfig.getProperties())
                .build();
        sessionFactory = hibernateConfig.buildSessionFactory(serviceRegistry);

        // Create JDBC connector config
        Map<String, String> props = new HashMap<>();
        props.put(JdbcSinkConnectorConfig.CONNECTION_URL, SNOWFLAKE_URL);
        props.put(JdbcSinkConnectorConfig.CONNECTION_USER, SNOWFLAKE_USER);
        props.put(JdbcSinkConnectorConfig.CONNECTION_PASSWORD, SNOWFLAKE_PASSWORD);
        props.put(JdbcSinkConnectorConfig.CONNECTION_DATABASE, SNOWFLAKE_DATABASE);
        props.put(JdbcSinkConnectorConfig.CONNECTION_SCHEMA, SNOWFLAKE_SCHEMA);
        JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(props);

        // Create dialect
        DatabaseDialectProvider provider = new SnowflakeDatabaseDialect.SnowflakeDatabaseDialectProvider();
        dialect = provider.instantiate(config, sessionFactory);

        // Create direct JDBC connection for testing
        Properties connectionProps = new Properties();
        connectionProps.put("user", SNOWFLAKE_USER);
        connectionProps.put("password", SNOWFLAKE_PASSWORD);
        connection = DriverManager.getConnection(SNOWFLAKE_URL, connectionProps);

        // Create test schema if it doesn't exist
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE SCHEMA IF NOT EXISTS " + SNOWFLAKE_SCHEMA);
        }
    }

    @AfterAll
    static void tearDown() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }

        if (sessionFactory != null && !sessionFactory.isClosed()) {
            sessionFactory.close();
        }
    }

    @Test
    @DisplayName("Should create and drop table")
    void testCreateAndDropTable() throws SQLException {
        // Create a test table
        TableId tableId = new TableId(SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA, "TEST_TABLE");
        String qualifiedTableName = "\"" + SNOWFLAKE_SCHEMA + "\".\"TEST_TABLE\"";
        String createTableSql = "CREATE TABLE " + qualifiedTableName + " (id INT, name VARCHAR(255))";

        try (Statement stmt = connection.createStatement()) {
            // Drop table if it exists
            stmt.execute("DROP TABLE IF EXISTS " + qualifiedTableName);

            // Create table
            stmt.execute(createTableSql);

            // Verify table exists
            boolean tableExists = false;
            try (java.sql.ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '" +
                            SNOWFLAKE_SCHEMA + "' AND TABLE_NAME = 'TEST_TABLE'")) {
                if (rs.next()) {
                    tableExists = rs.getInt(1) > 0;
                }
            }
            assertThat(tableExists).isTrue();

            // Drop table
            stmt.execute("DROP TABLE " + qualifiedTableName);

            // Verify table no longer exists
            tableExists = false;
            try (java.sql.ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '" +
                            SNOWFLAKE_SCHEMA + "' AND TABLE_NAME = 'TEST_TABLE'")) {
                if (rs.next()) {
                    tableExists = rs.getInt(1) > 0;
                }
            }
            assertThat(tableExists).isFalse();
        }
    }

    @Test
    @DisplayName("Should get database version")
    void testGetDatabaseVersion() {
        assertThat(dialect.getVersion()).isNotNull();
        assertThat(dialect.getVersion().getMajor()).isGreaterThan(0);
    }

    // Database timezone test is not applicable for this dialect
    // as it doesn't expose the getDatabaseTimeZone method publicly
}
