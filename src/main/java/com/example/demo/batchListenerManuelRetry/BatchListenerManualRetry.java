package com.example.demo.batchListenerManuelRetry;

import com.example.demo.base.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.BatchAcknowledgingMessageListener;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.List;


/**
 * Batch listener implementation with manual retry logic.
 *
 * <p>Key characteristics:
 * <ul>
 *   <li>Processes messages in batches using {@link BatchAcknowledgingMessageListener}</li>
 *   <li>Manual acknowledgment mode for at-least-once semantics</li>
 *   <li>Manual retry logic within batch - retries individual records, not entire batch</li>
 *   <li>Handles deserialization errors gracefully (null values)</li>
 *   <li>Retryable exceptions: retry up to 3 times with 500ms backoff</li>
 *   <li>Non-retryable exceptions: skip immediately and continue</li>
 * </ul>
 */
@Slf4j
@Component
@Profile("batchListenerManuelRetry")
public class BatchListenerManualRetry extends AbstractMessageListener {

    private final ConsumerFactory<String, DocumentOperation> consumerFactory;
    private final CommonErrorHandler errorHandler;

    public BatchListenerManualRetry(ConsumerFactory<String, DocumentOperation> consumerFactory,
                                    CommonErrorHandler errorHandler,
                                    MessageProcessor messageProcessor) {
        super(messageProcessor);
        this.consumerFactory = consumerFactory;
        this.errorHandler = errorHandler;
        start();
    }


    @Override
    protected ConcurrentMessageListenerContainer<String, DocumentOperation> createListenerContainer() {
        ContainerProperties containerProperties = new ContainerProperties("input-topic");
        containerProperties.setGroupId("test-group");

        // MANUAL mode for at-least-once semantics
        // Important: Error handler will set setAckAfterHandle(true) and setCommitRecovered(true)
        containerProperties.setAckMode(ContainerProperties.AckMode.MANUAL);

        // Use BATCH listener - this is the key! With batch listeners, null values from
        // deserialization errors ARE passed to the listener (unlike record-level listeners)
        containerProperties.setMessageListener(
            (BatchAcknowledgingMessageListener<String, DocumentOperation>) this::handleBatch);

        var newContainer = new ConcurrentMessageListenerContainer<>(consumerFactory, containerProperties);
        newContainer.setCommonErrorHandler(errorHandler);
        return newContainer;
    }


    private void handleBatch(List<ConsumerRecord<String, DocumentOperation>> records,
                             Acknowledgment acknowledgment) {
        for (ConsumerRecord<String, DocumentOperation> data : records) {

            // STEP 1: Check for DESERIALIZATION errors (pre-listener, not retryable)
            if (data.value() != null) {
                // STEP 2: Try to process the record with retry logic for retryable exceptions
                processRecordWithRetry(data);
            } else {
                // Deserialization errors are NOT retryable - data is corrupted
                // Continue to next record - don't throw exception
                processDeserializationError(data);
            }
        }

        // Acknowledge all records in batch after successful processing
        // At-least-once: If processing fails before this line, offset not committed → redelivery
        acknowledgment.acknowledge();
    }

    /**
     * Process a single record with manual retry logic.
     * - RetryableException: retry up to 3 times with 500ms backoff
     * - NotRetryableException: skip and continue
     * - Other exceptions: treat as retryable
     */
    private void processRecordWithRetry(ConsumerRecord<String, DocumentOperation> data) {
        int maxAttempts = 3;
        long backoffMs = 500;

        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                processRecord(data);
                return; // Success - exit retry loop
            } catch (NotRetryableException e) {
                // Not retryable - log and stop trying this record
                log.error(">>> Non-retryable error processing record [{}], skipping: {}",
                          data.key(), ErrorHandlerConfig.exceptionAsString(e));
                return; // Exit without incrementing processedCount
            } catch (RetryableException e) {
                // Retryable exception - log and retry if attempts remain
                if (attempt < maxAttempts) {
                    log.warn("Retrying ({} attempt) for key '{}': \n\t{}",
                             attempt, data.key(), ErrorHandlerConfig.exceptionAsString(e));
                    try {
                        Thread.sleep(backoffMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        log.error("Retry interrupted for key '{}'", data.key());
                        return;
                    }
                } else {
                    // Max attempts reached - send to recoverer (just log for now)
                    log.error("""

                              ***
                              *** Message processing failed after all retries. Sending to nowhere (just logging for now).
                              *** Record: [{}-{}@{}] Key: '{}' - skipping
                              *** Error: {}
                              *** Value:
                              {}
                              ***
                              """,
                              data.topic(), data.partition(), data.offset(), data.key(),
                              ErrorHandlerConfig.exceptionAsString(e),
                              data.value());
                    return; // Exit without incrementing processedCount
                }
            } catch (Exception e) {
                // Other exceptions - treat as retryable by wrapping
                if (attempt < maxAttempts) {
                    log.warn("Retrying ({} attempt) for key '{}' due to unexpected error: \n\t{}",
                             attempt, data.key(), ErrorHandlerConfig.exceptionAsString(e));
                    try {
                        Thread.sleep(backoffMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        log.error("Retry interrupted for key '{}'", data.key());
                        return;
                    }
                } else {
                    log.error(">>> Unexpected error processing record [{}] after {} attempts, skipping: {}",
                              data.key(), maxAttempts, ErrorHandlerConfig.exceptionAsString(e));
                    return;
                }
            }
        }
    }

}
