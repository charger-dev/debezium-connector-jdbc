/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.snowflake;

import static org.fest.assertions.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.dialect.DatabaseDialectProvider;

import net.snowflake.hibernate.dialect.SnowflakeDialect;

/**
 * Unit tests for the {@link SnowflakeDatabaseDialect#toSafeIdentifier} method.
 *
 * @author Kevin Cui
 */
@Tag("UnitTests")
public class SnowflakeToSafeIdentifierTest {

    private SnowflakeDatabaseDialect dialect;
    private Method toSafeIdentifierMethod;

    @BeforeEach
    void setUp() throws Exception {
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

        // Create the dialect using the provider
        DatabaseDialectProvider provider = new SnowflakeDatabaseDialect.SnowflakeDatabaseDialectProvider();
        dialect = (SnowflakeDatabaseDialect) provider.instantiate(config, sessionFactory);

        // Get access to the private toSafeIdentifier method using reflection
        toSafeIdentifierMethod = SnowflakeDatabaseDialect.class.getDeclaredMethod("toSafeIdentifier", String.class);
        toSafeIdentifierMethod.setAccessible(true);
    }

    private String invokeToSafeIdentifier(String identifier) throws Exception {
        try {
            return (String) toSafeIdentifierMethod.invoke(dialect, identifier);
        }
        catch (InvocationTargetException e) {
            if (e.getCause() instanceof IllegalArgumentException) {
                throw (IllegalArgumentException) e.getCause();
            }
            throw e;
        }
    }

    @Test
    @DisplayName("Should throw IllegalArgumentException for null identifier")
    void testNullIdentifier() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            invokeToSafeIdentifier(null);
        });
        assertThat(exception.getMessage()).isEqualTo("Column name cannot be null or empty");
    }

    @Test
    @DisplayName("Should throw IllegalArgumentException for empty identifier")
    void testEmptyIdentifier() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            invokeToSafeIdentifier("");
        });
        assertThat(exception.getMessage()).isEqualTo("Column name cannot be null or empty");
    }

    @Test
    @DisplayName("Should not quote valid uppercase identifiers")
    void testValidUppercaseIdentifiers() throws Exception {
        // Valid identifiers in Snowflake start with a letter or underscore and contain only letters, numbers, and underscores
        String[] validIdentifiers = {
                "VALID_COLUMN",
                "COLUMN1",
                "_COLUMN",
                "A_B_C",
                "COLUMN_123"
        };

        for (String identifier : validIdentifiers) {
            String result = invokeToSafeIdentifier(identifier);
            assertThat(result).isEqualTo(identifier);
        }
    }

    @Test
    @DisplayName("Should quote lowercase identifiers")
    void testLowercaseIdentifiers() throws Exception {
        // Lowercase identifiers should be quoted and converted to uppercase
        String[] lowercaseIdentifiers = {
                "lowercase_column",
                "column1",
                "_column",
                "a_b_c"
        };

        for (String identifier : lowercaseIdentifiers) {
            String result = invokeToSafeIdentifier(identifier);
            assertThat(result).isEqualTo("\"" + identifier.toUpperCase() + "\"");
        }
    }

    @Test
    @DisplayName("Should quote mixed case identifiers")
    void testMixedCaseIdentifiers() throws Exception {
        // Mixed case identifiers should be quoted and converted to uppercase
        String[] mixedCaseIdentifiers = {
                "MixedCase_Column",
                "Column1",
                "_Column",
                "A_b_C"
        };

        for (String identifier : mixedCaseIdentifiers) {
            String result = invokeToSafeIdentifier(identifier);
            assertThat(result).isEqualTo("\"" + identifier.toUpperCase() + "\"");
        }
    }

    @Test
    @DisplayName("Should quote identifiers with special characters")
    void testIdentifiersWithSpecialCharacters() throws Exception {
        // Identifiers with special characters should be quoted
        String[] specialCharIdentifiers = {
                "column-name",
                "column.name",
                "column@name",
                "column#name",
                "column$name",
                "column%name",
                "column&name",
                "column*name",
                "column(name)",
                "column+name",
                "column=name",
                "column:name",
                "column;name",
                "column,name",
                "column<name>",
                "column/name",
                "column\\name",
                "column|name",
                "column?name",
                "column!name",
                "column^name",
                "column~name",
                "column`name",
                "column'name",
                "column\"name",
                "column[name]",
                "column{name}",
                "column space name"
        };

        for (String identifier : specialCharIdentifiers) {
            String result = invokeToSafeIdentifier(identifier);
            assertThat(result).isEqualTo("\"" + identifier.toUpperCase() + "\"");
        }
    }

    @Test
    @DisplayName("Should quote identifiers starting with numbers")
    void testIdentifiersStartingWithNumbers() throws Exception {
        // Identifiers starting with numbers should be quoted
        String[] numericStartIdentifiers = {
                "1column",
                "123_column",
                "2ndColumn"
        };

        for (String identifier : numericStartIdentifiers) {
            String result = invokeToSafeIdentifier(identifier);
            assertThat(result).isEqualTo("\"" + identifier.toUpperCase() + "\"");
        }
    }

    @Test
    @DisplayName("Should quote reserved keywords")
    void testReservedKeywords() throws Exception {
        // Reserved keywords should be quoted
        String[] reservedKeywords = {
                "table",
                "select",
                "from",
                "where",
                "join",
                "group",
                "order",
                "by",
                "having",
                "insert",
                "update",
                "delete",
                "create",
                "alter",
                "drop",
                "schema",
                "constraint",
                "primary",
                "unique",
                "check",
                "null",
                "not",
                "and",
                "or",
                "in",
                "between",
                "like",
                "is",
                "as",
                "case",
                "when",
                "then",
                "else",
                "distinct",
                "all",
                "union",
                "exists",
                "grant",
                "set",
                "for",
                "of",
                "on",
                "to",
                "values",
                "with"
        };

        for (String keyword : reservedKeywords) {
            String result = invokeToSafeIdentifier(keyword);
            // The method should quote all reserved keywords
            assertThat(result).isEqualTo("\"" + keyword.toUpperCase() + "\"");
        }
    }

    @Test
    @DisplayName("Should handle extremely long identifiers")
    void testExtremelyLongIdentifiers() throws Exception {
        // Create an identifier that's very long (over 100 characters)
        StringBuilder longIdentifierBuilder = new StringBuilder();
        for (int i = 0; i < 10; i++) {
            longIdentifierBuilder.append("VERY_LONG_IDENTIFIER_PART_");
        }
        String longIdentifier = longIdentifierBuilder.toString();

        // Test the long identifier
        String result = invokeToSafeIdentifier(longIdentifier);
        assertThat(result).isEqualTo(longIdentifier); // Should not be quoted as it's valid uppercase

        // Test a long mixed-case identifier
        String longMixedCaseIdentifier = longIdentifier.toLowerCase();
        String mixedCaseResult = invokeToSafeIdentifier(longMixedCaseIdentifier);
        assertThat(mixedCaseResult).isEqualTo("\"" + longMixedCaseIdentifier.toUpperCase() + "\"");
    }

    @Test
    @DisplayName("Should handle identifiers with only numbers")
    void testIdentifiersWithOnlyNumbers() throws Exception {
        String[] numericIdentifiers = {
                "123",
                "456789",
                "0"
        };

        for (String identifier : numericIdentifiers) {
            String result = invokeToSafeIdentifier(identifier);
            assertThat(result).isEqualTo("\"" + identifier.toUpperCase() + "\"");
        }
    }

    @Test
    @DisplayName("Should handle identifiers with Unicode characters")
    void testIdentifiersWithUnicodeCharacters() throws Exception {
        String[] unicodeIdentifiers = {
                "column_with_é",
                "column_with_ñ",
                "column_with_ü",
                "column_with_ç",
                "column_with_ß",
                "column_with_ø",
                "column_with_å",
                "column_with_æ",
                "column_with_œ"
        };

        for (String identifier : unicodeIdentifiers) {
            String result = invokeToSafeIdentifier(identifier);
            assertThat(result).isEqualTo("\"" + identifier.toUpperCase() + "\"");
        }
    }

    @Test
    @DisplayName("Should handle identifiers with whitespace")
    void testIdentifiersWithWhitespace() throws Exception {
        String[] whitespaceIdentifiers = {
                "column with spaces",
                "column\twith\ttabs",
                "column\nwith\nnewlines",
                "column\rwith\rcarriage\rreturns",
                "column with multiple     spaces",
                " column with leading space",
                "column with trailing space ",
                " column with both leading and trailing space "
        };

        for (String identifier : whitespaceIdentifiers) {
            String result = invokeToSafeIdentifier(identifier);
            assertThat(result).isEqualTo("\"" + identifier.toUpperCase() + "\"");
        }
    }
}
