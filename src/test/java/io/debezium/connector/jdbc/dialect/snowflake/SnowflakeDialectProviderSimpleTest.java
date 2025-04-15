/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.snowflake;

import static org.fest.assertions.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import org.hibernate.dialect.Dialect;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.connector.jdbc.dialect.DatabaseDialectProvider;
import io.debezium.connector.jdbc.dialect.snowflake.SnowflakeDatabaseDialect.SnowflakeDatabaseDialectProvider;

import net.snowflake.hibernate.dialect.SnowflakeDialect;

/**
 * Simple unit tests for the {@link SnowflakeDatabaseDialectProvider} class.
 * These tests don't require a SessionFactory.
 *
 * @author Kevin Cui
 */
@Tag("UnitTests")
public class SnowflakeDialectProviderSimpleTest {

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
        Dialect nonSnowflakeDialect = mock(Dialect.class);
        assertThat(provider.supports(nonSnowflakeDialect)).isFalse();
    }

    @Test
    @DisplayName("Should return correct dialect class name")
    void testReturnsCorrectDialectClassName() {
        DatabaseDialectProvider provider = new SnowflakeDatabaseDialectProvider();
        assertThat(provider.name()).isEqualTo(SnowflakeDatabaseDialect.class);
    }
}
