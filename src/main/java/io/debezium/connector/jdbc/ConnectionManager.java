/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.connect.errors.ConnectException;
import org.hibernate.SessionFactory;
import org.hibernate.StatelessSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectionManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionManager.class);
    private final SessionFactory sessionFactory;
    private final int maxRetries;
    private final long retryDelayMillis;

    // Thread-local storage for session reuse
    private final ThreadLocal<StatelessSession> threadLocalSession = new ThreadLocal<>();

    // Track open sessions for cleanup
    private final ConcurrentHashMap<StatelessSession, Boolean> openSessions = new ConcurrentHashMap<>();

    /**
     * Creates a new ConnectionManager instance with default retry settings.
     *
     * @param sessionFactory the Hibernate SessionFactory to use for creating connections
     */
    public ConnectionManager(SessionFactory sessionFactory) {
        this(sessionFactory, 5, 2000);
    }

    /**
     * Creates a new ConnectionManager instance with custom retry settings.
     *
     * @param sessionFactory the Hibernate SessionFactory to use for creating connections
     * @param maxRetries the maximum number of retry attempts
     * @param retryDelayMillis the base delay between retry attempts in milliseconds
     */
    public ConnectionManager(SessionFactory sessionFactory, int maxRetries, long retryDelayMillis) {
        this.sessionFactory = sessionFactory;
        this.maxRetries = maxRetries;
        this.retryDelayMillis = retryDelayMillis;
    }

    /**
     * Gets a stateless session from the thread-local cache or creates a new one if needed.
     * This method is preferred over openStatelessSession() as it reuses sessions when possible.
     *
     * @return a StatelessSession that can be reused within the same thread
     * @throws RuntimeException if unable to open a session after retries
     */
    public StatelessSession getStatelessSession() {
        StatelessSession session = threadLocalSession.get();
        if (session == null || !isSessionValid(session)) {
            session = createNewStatelessSession();
            threadLocalSession.set(session);
            openSessions.put(session, Boolean.TRUE);
            LOGGER.debug("Created new StatelessSession for thread {}", Thread.currentThread().getName());
        }
        return session;
    }

    /**
     * Checks if a session is still valid and usable.
     *
     * @param session the session to check
     * @return true if the session is valid, false otherwise
     */
    private boolean isSessionValid(StatelessSession session) {
        try {
            // A simple check to see if the session is still usable
            session.isOpen();
            return true;
        }
        catch (Exception e) {
            LOGGER.debug("Session is no longer valid: {}", e.getMessage());
            return false;
        }
    }

    /**
     * Closes the thread-local session if it exists.
     */
    public void closeThreadLocalSession() {
        StatelessSession session = threadLocalSession.get();
        if (session != null) {
            try {
                if (session.isOpen()) {
                    session.close();
                }
            }
            catch (Exception e) {
                LOGGER.warn("Error closing thread-local session: {}", e.getMessage());
            }
            finally {
                threadLocalSession.remove();
                openSessions.remove(session);
            }
        }
    }

    /**
     * Closes all open sessions managed by this ConnectionManager.
     */
    public void closeAllSessions() {
        LOGGER.info("Closing all open sessions (count: {})", openSessions.size());
        for (StatelessSession session : openSessions.keySet()) {
            try {
                if (session.isOpen()) {
                    session.close();
                }
            }
            catch (Exception e) {
                LOGGER.warn("Error closing session: {}", e.getMessage());
            }
        }
        openSessions.clear();
        threadLocalSession.remove();
    }

    /**
     * Opens a stateless session with retry logic for handling connection failures.
     * For backward compatibility - consider using getStatelessSession() instead for better performance.
     *
     * @return a StatelessSession
     * @throws RuntimeException if unable to open a session after retries
     */
    public StatelessSession openStatelessSession() {
        return createNewStatelessSession();
    }

    /**
     * Creates a new stateless session with retry logic.
     *
     * @return a new StatelessSession
     * @throws RuntimeException if unable to open a session after retries
     */
    private StatelessSession createNewStatelessSession() {
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                return sessionFactory.openStatelessSession();
            }
            catch (Exception e) {
                String errorMessage = e.getMessage();
                boolean isAuthError = errorMessage != null &&
                        (errorMessage.contains("Session no longer exists") ||
                                errorMessage.contains("authentication failed") ||
                                errorMessage.contains("login required"));

                if (isAuthError) {
                    LOGGER.error("Snowflake authentication error detected, attempt {} of {}: {}",
                            attempt, maxRetries, errorMessage);

                    // For authentication errors, we need to close and recreate the session factory
                    if (attempt == maxRetries) {
                        LOGGER.error("Maximum authentication retry attempts reached. Service needs to be restarted.");
                        throw new ConnectException("Snowflake authentication failed after maximum retry attempts. " +
                                "Please check your credentials and connection settings.", e);
                    }
                }
                else {
                    LOGGER.error("Failed to open StatelessSession, attempt {} of {}", attempt, maxRetries, e);
                }

                try {
                    // Exponential backoff for retries
                    long delay = retryDelayMillis * (long) Math.pow(2, attempt - 1);
                    Thread.sleep(delay);
                }
                catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        throw new RuntimeException("Failed to open StatelessSession after retries.");
    }

    /**
     * Checks if the session factory is still open.
     *
     * @return true if the session factory is open, false otherwise
     */
    public boolean isSessionFactoryOpen() {
        return sessionFactory != null && sessionFactory.isOpen();
    }

    /**
     * Closes the session factory if it's open.
     * Also closes all open sessions managed by this ConnectionManager.
     * Catches and logs any exceptions that occur during close.
     */
    public void closeSessionFactory() {
        // First close all open sessions
        closeAllSessions();

        if (isSessionFactoryOpen()) {
            LOGGER.info("Closing the session factory");
            try {
                sessionFactory.close();
            }
            catch (Exception e) {
                LOGGER.error("Error occurred while closing session factory", e);
                // Don't rethrow the exception - we want to continue cleanup
            }
        }
        else {
            LOGGER.info("Session factory already closed");
        }
    }
}
