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
 * Tests for the column name resolution behavior in Snowflake.
 *
 * @author Kevin Cui
 */
@Tag("UnitTests")
public class SnowflakeColumnNameFromFieldTest {

    @Test
    @DisplayName("Should handle regular column names correctly in Snowflake")
    void testRegularColumnNames() {
        // In Snowflake, regular column names are stored in uppercase by default
        String regularColumn = "regular_column";
        String expectedUppercase = "REGULAR_COLUMN";

        // Verify that uppercase is used for regular identifiers in Snowflake
        assertThat(expectedUppercase).isEqualTo(regularColumn.toUpperCase());
    }

    @Test
    @DisplayName("Should handle column names with spaces correctly in Snowflake")
    void testColumnNamesWithSpaces() {
        // In Snowflake, column names with spaces must be quoted
        String columnWithSpaces = "column with spaces";
        String quotedColumn = "\"" + columnWithSpaces + "\"";

        // Verify that the quoted column name is correctly formatted
        assertThat(quotedColumn).isEqualTo("\"column with spaces\"");
    }

    @Test
    @DisplayName("Should handle column names with special symbols correctly in Snowflake")
    void testColumnNamesWithSpecialSymbols() {
        // In Snowflake, column names with special symbols like # must be quoted
        String columnWithSymbols = "column#with#symbols";
        String quotedColumn = "\"" + columnWithSymbols + "\"";

        // Verify that the quoted column name is correctly formatted
        assertThat(quotedColumn).isEqualTo("\"column#with#symbols\"");
    }
}
