package com.example.demo;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.BatchAcknowledgingMessageListener;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.serializer.SerializationUtils;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Component
public class KafkaInputChannel {

    private final ConsumerFactory<String, DocumentOperation> consumerFactory;
    private final CommonErrorHandler errorHandler;
    private final MessageProcessor messageProcessor;
    public final AtomicInteger processedCount = new AtomicInteger(0);
    public final AtomicInteger errorCount = new AtomicInteger(0);

    public KafkaInputChannel(ConsumerFactory<String, DocumentOperation> consumerFactory,
                             CommonErrorHandler errorHandler,
                             MessageProcessor messageProcessor) {
        this.consumerFactory = consumerFactory;
        this.errorHandler = errorHandler;
        this.messageProcessor = messageProcessor;
        createListenerContainer();
    }

    private void createListenerContainer() {
        ContainerProperties containerProperties = new ContainerProperties("input-topic");
        containerProperties.setGroupId("test-group");

        // MANUAL mode for at-least-once semantics
        // Important: Error handler will set setAckAfterHandle(true) and setCommitRecovered(true)
        containerProperties.setAckMode(ContainerProperties.AckMode.MANUAL);

        // Use BATCH listener - this is the key! With batch listeners, null values from
        // deserialization errors ARE passed to the listener (unlike record-level listeners)
        containerProperties.setMessageListener(
            (BatchAcknowledgingMessageListener<String, DocumentOperation>) this::handleBatch);

        ConcurrentMessageListenerContainer<String, DocumentOperation> container =
            new ConcurrentMessageListenerContainer<>(consumerFactory, containerProperties);

        container.setCommonErrorHandler(errorHandler);

        container.start();
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
            } catch (ErrorHandlerConfig.NotRetryableException e) {
                // Not retryable - log and stop trying this record
                log.error(">>> Non-retryable error processing record [{}], skipping: {}",
                          data.key(), ErrorHandlerConfig.exceptionAsString(e));
                return; // Exit without incrementing processedCount
            } catch (ErrorHandlerConfig.RetryableException e) {
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


    private void processRecord(ConsumerRecord<String, DocumentOperation> data) {
        messageProcessor.process(data);
        processedCount.incrementAndGet();
    }

    private void processDeserializationError(ConsumerRecord<String, DocumentOperation> data) {
        Exception ex = SerializationUtils.getExceptionFromHeader(
                data,
                SerializationUtils.VALUE_DESERIALIZER_EXCEPTION_HEADER,
                new LogAccessor(KafkaInputChannel.class));

        if (ex != null) {
            log.error(">>> Deserialization error (NOT retryable) - topic: {}, partition: {}, offset: {}, error: {}",
                      data.topic(), data.partition(), data.offset(), ex.getMessage());
            errorCount.incrementAndGet();
        }

        // Notify the message processor about the deserialization error
        messageProcessor.processDeserializationError(data, ex);
    }
}
