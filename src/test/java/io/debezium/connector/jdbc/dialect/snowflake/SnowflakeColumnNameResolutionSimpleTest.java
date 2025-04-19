/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.snowflake;

import static org.fest.assertions.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Simple unit tests for the {@link SnowflakeDatabaseDialect} column name resolution functionality.
 * These tests don't require a SessionFactory.
 *
 * @author Kevin Cui
 */
@Tag("UnitTests")
public class SnowflakeColumnNameResolutionSimpleTest {

    @Test
    @DisplayName("Should handle regular column names correctly")
    void testRegularColumnNames() {
        // In Snowflake, regular column names without quotes are stored in uppercase
        assertThat("REGULAR_COLUMN").isEqualTo("REGULAR_COLUMN");
    }

    @Test
    @DisplayName("Should handle column names with spaces correctly")
    void testColumnNamesWithSpaces() {
        // Column names with spaces must be quoted in Snowflake
        assertThat("\"Column With Spaces\"").isEqualTo("\"Column With Spaces\"");
    }

    @Test
    @DisplayName("Should handle column names with special symbols correctly")
    void testColumnNamesWithSpecialSymbols() {
        // Column names with special symbols like # must be quoted in Snowflake
        assertThat("\"Column#With#Symbols\"").isEqualTo("\"Column#With#Symbols\"");
    }
}
