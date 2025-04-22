/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.kafka.connect.errors.ConnectException;
import org.hibernate.SessionFactory;
import org.hibernate.StatelessSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the {@link ConnectionManager} class.
 *
 * @author Kevin Cui
 */
@Tag("UnitTests")
public class ConnectionManagerTest {

    private SessionFactory sessionFactory;
    private StatelessSession statelessSession;
    private ConnectionManager connectionManager;

    @BeforeEach
    void setUp() {
        sessionFactory = mock(SessionFactory.class);
        statelessSession = mock(StatelessSession.class);
        // Use shorter retry delay for faster test execution
        connectionManager = new ConnectionManager(sessionFactory, 5, 10);
    }

    @Test
    @DisplayName("Should successfully open a stateless session")
    void testOpenStatelessSessionSuccess() {
        // Arrange
        when(sessionFactory.openStatelessSession()).thenReturn(statelessSession);

        // Act
        StatelessSession result = connectionManager.openStatelessSession();

        // Assert
        assertNotNull(result);
        assertEquals(statelessSession, result);
        verify(sessionFactory, times(1)).openStatelessSession();
    }

    @Test
    @DisplayName("Should retry when session creation fails with a non-authentication error")
    void testOpenStatelessSessionRetryOnGenericError() {
        // Arrange
        RuntimeException genericError = new RuntimeException("Generic database error");

        // First call throws error, second call succeeds
        when(sessionFactory.openStatelessSession())
                .thenThrow(genericError)
                .thenReturn(statelessSession);

        // Act
        StatelessSession result = connectionManager.openStatelessSession();

        // Assert
        assertNotNull(result);
        assertEquals(statelessSession, result);
        verify(sessionFactory, times(2)).openStatelessSession();
    }

    @Test
    @DisplayName("Should throw ConnectException after max retries on authentication error")
    void testOpenStatelessSessionThrowsOnAuthError() {
        // Arrange
        RuntimeException authError = new RuntimeException("Session no longer exists. New login required to access the service");

        // All calls throw authentication error
        when(sessionFactory.openStatelessSession()).thenThrow(authError);

        // Act & Assert
        ConnectException exception = assertThrows(ConnectException.class, () -> {
            connectionManager.openStatelessSession();
        });

        assertTrue(exception.getMessage().contains("Snowflake authentication failed"));
        verify(sessionFactory, times(5)).openStatelessSession(); // Should try 5 times (max retries)
    }

    @Test
    @DisplayName("Should detect various authentication error messages")
    void testDetectsVariousAuthErrors() {
        // Arrange
        RuntimeException authError1 = new RuntimeException("Session no longer exists");
        RuntimeException authError2 = new RuntimeException("authentication failed");
        RuntimeException authError3 = new RuntimeException("login required");

        // Different calls throw different auth errors
        when(sessionFactory.openStatelessSession())
                .thenThrow(authError1)
                .thenThrow(authError2)
                .thenThrow(authError3)
                .thenReturn(statelessSession);

        // Act
        StatelessSession result = connectionManager.openStatelessSession();

        // Assert
        assertNotNull(result);
        assertEquals(statelessSession, result);
        verify(sessionFactory, times(4)).openStatelessSession();
    }

    @Test
    @DisplayName("Should throw RuntimeException after max retries on generic error")
    void testOpenStatelessSessionThrowsAfterMaxRetries() {
        // Arrange
        RuntimeException genericError = new RuntimeException("Generic database error");

        // All calls throw generic error
        when(sessionFactory.openStatelessSession()).thenThrow(genericError);

        // Act & Assert
        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            connectionManager.openStatelessSession();
        });

        assertEquals("Failed to open StatelessSession after retries.", exception.getMessage());
        verify(sessionFactory, times(5)).openStatelessSession(); // Should try 5 times (max retries)
    }

    @Test
    @DisplayName("Should correctly check if session factory is open")
    void testIsSessionFactoryOpen() {
        // Arrange
        when(sessionFactory.isOpen()).thenReturn(true, false);

        // Act & Assert
        assertTrue(connectionManager.isSessionFactoryOpen());

        // Second call should return false
        when(sessionFactory.isOpen()).thenReturn(false);
        assertFalse(connectionManager.isSessionFactoryOpen());
    }

    @Test
    @DisplayName("Should close session factory if it's open")
    void testCloseSessionFactoryWhenOpen() {
        // Arrange
        when(sessionFactory.isOpen()).thenReturn(true);
        doNothing().when(sessionFactory).close();

        // Act
        connectionManager.closeSessionFactory();

        // Assert
        verify(sessionFactory, times(1)).isOpen();
        verify(sessionFactory, times(1)).close();
    }

    @Test
    @DisplayName("Should not close session factory if it's already closed")
    void testCloseSessionFactoryWhenClosed() {
        // Arrange
        when(sessionFactory.isOpen()).thenReturn(false);

        // Act
        connectionManager.closeSessionFactory();

        // Assert
        verify(sessionFactory, times(1)).isOpen();
        verify(sessionFactory, times(0)).close(); // Should not call close
    }

    @Test
    @DisplayName("Should handle exception during session factory close")
    void testHandleExceptionDuringClose() {
        // Arrange
        when(sessionFactory.isOpen()).thenReturn(true);
        doThrow(new RuntimeException("Error closing session factory")).when(sessionFactory).close();

        // Act & Assert - should not throw exception
        connectionManager.closeSessionFactory();

        verify(sessionFactory, times(1)).isOpen();
        verify(sessionFactory, times(1)).close();
    }

    @Test
    @DisplayName("Should create ConnectionManager with custom retry settings")
    void testCustomRetrySettings() {
        // Arrange
        int customMaxRetries = 3;
        long customRetryDelay = 100;
        RuntimeException error = new RuntimeException("Database error");

        // First two calls throw error, third call succeeds
        when(sessionFactory.openStatelessSession())
                .thenThrow(error)
                .thenThrow(error)
                .thenReturn(statelessSession);

        ConnectionManager customManager = new ConnectionManager(sessionFactory, customMaxRetries, customRetryDelay);

        // Act
        StatelessSession result = customManager.openStatelessSession();

        // Assert
        assertNotNull(result);
        assertEquals(statelessSession, result);
        verify(sessionFactory, times(3)).openStatelessSession(); // Should try exactly 3 times
    }

    @Test
    @DisplayName("Should reuse session when calling getStatelessSession multiple times")
    void testGetStatelessSessionReusesSession() {
        // Arrange
        when(sessionFactory.openStatelessSession()).thenReturn(statelessSession);
        when(statelessSession.isOpen()).thenReturn(true);

        // Act
        StatelessSession session1 = connectionManager.getStatelessSession();
        StatelessSession session2 = connectionManager.getStatelessSession();

        // Assert
        assertNotNull(session1);
        assertSame(session1, session2); // Should be the same object instance
        verify(sessionFactory, times(1)).openStatelessSession(); // Should only create one session
    }

    @Test
    @DisplayName("Should create new session when existing one is invalid")
    void testGetStatelessSessionCreatesNewSessionWhenInvalid() {
        // Arrange
        StatelessSession session1 = mock(StatelessSession.class);
        StatelessSession session2 = mock(StatelessSession.class);

        when(sessionFactory.openStatelessSession()).thenReturn(session1, session2);
        when(session1.isOpen()).thenThrow(new RuntimeException("Session is invalid"));
        when(session2.isOpen()).thenReturn(true);

        // Act
        StatelessSession result1 = connectionManager.getStatelessSession();
        StatelessSession result2 = connectionManager.getStatelessSession();

        // Assert
        assertNotNull(result1);
        assertNotNull(result2);
        assertEquals(session1, result1);
        assertEquals(session2, result2);
        verify(sessionFactory, times(2)).openStatelessSession(); // Should create two sessions
    }

    @Test
    @DisplayName("Should close thread-local session")
    void testCloseThreadLocalSession() {
        // Arrange
        when(sessionFactory.openStatelessSession()).thenReturn(statelessSession);
        when(statelessSession.isOpen()).thenReturn(true);

        // Act
        connectionManager.getStatelessSession(); // Create and store a session
        connectionManager.closeThreadLocalSession();

        // Assert
        verify(statelessSession, times(1)).close();

        // Getting a new session should create a new one
        connectionManager.getStatelessSession();
        verify(sessionFactory, times(2)).openStatelessSession();
    }

    @Test
    @DisplayName("Should close all sessions when closing session factory")
    void testCloseAllSessionsWhenClosingSessionFactory() {
        // Arrange
        when(sessionFactory.openStatelessSession()).thenReturn(statelessSession);
        when(sessionFactory.isOpen()).thenReturn(true);
        when(statelessSession.isOpen()).thenReturn(true);

        // Act
        connectionManager.getStatelessSession(); // Create and store a session
        connectionManager.closeSessionFactory();

        // Assert
        verify(statelessSession, times(1)).close();
        verify(sessionFactory, times(1)).close();
    }
}
