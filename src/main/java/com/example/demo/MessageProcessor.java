package com.example.demo;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Interface for processing Kafka messages.
 * Implementations can define custom processing logic and throw exceptions
 * to trigger retry or recovery mechanisms.
 */
public interface MessageProcessor {

    /**
     * Process a single Kafka message.
     *
     * @param record The consumer record to process
     * @throws ErrorHandlerConfig.RetryableException for transient errors that should be retried
     * @throws ErrorHandlerConfig.NotRetryableException for permanent errors that should not be retried
     */
    void process(ConsumerRecord<String, DocumentOperation> record);

    /**
     * Handle a record that failed deserialization.
     * Called when the record value is null due to deserialization errors.
     * The exception details can be extracted from the record headers.
     *
     * @param record The consumer record with null value
     * @param exception The deserialization exception (may be null if not found in headers)
     */
    void processDeserializationError(ConsumerRecord<String, DocumentOperation> record, Exception exception);
}
