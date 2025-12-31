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
    public final AtomicInteger processedCount = new AtomicInteger(0);
    public final AtomicInteger errorCount = new AtomicInteger(0);

    public KafkaInputChannel(ConsumerFactory<String, DocumentOperation> consumerFactory, CommonErrorHandler  errorHandler) {
        this.consumerFactory = consumerFactory;
        this.errorHandler = errorHandler;
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
                processRecord(data);
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


    private void processRecord(ConsumerRecord<String, DocumentOperation> data) {
        log.info("Successfully processed record: {}", data.value());
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
    }
}
