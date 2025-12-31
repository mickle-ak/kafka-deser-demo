package com.example.demo.base;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Common interface for all Kafka message listener implementations.
 * This allows different strategies for handling messages and errors to be tested and compared.
 */
public interface MessageListener {

    /**
     * Get the counter for successfully processed messages.
     * Used for testing and monitoring.
     *
     * @return Counter of processed messages
     */
    AtomicInteger getProcessedCount();

    /**
     * Get the counter for deserialization errors.
     * Used for testing and monitoring.
     *
     * @return Counter of deserialization errors
     */
    AtomicInteger getErrorCount();

    /**
     * Start the listener container.
     * Should be called after construction to begin consuming messages.
     */
    void start();

    /**
     * Stop the listener container.
     * Should be called during shutdown to gracefully stop consuming messages.
     */
    void stop();

    /**
     * Get a descriptive name for this implementation.
     * Useful for logging and debugging to identify which strategy is in use.
     *
     * @return Implementation name (e.g., "BatchListenerManualRetry")
     */
    String getImplementationName();
}
