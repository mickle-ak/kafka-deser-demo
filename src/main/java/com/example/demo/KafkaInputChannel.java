package com.example.demo;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.serializer.SerializationUtils;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Component
public class KafkaInputChannel {

    private final ConsumerFactory<String, DocumentOperation> consumerFactory;
    public final AtomicInteger processedCount = new AtomicInteger(0);
    public final AtomicInteger errorCount = new AtomicInteger(0);

    public KafkaInputChannel(ConsumerFactory<String, DocumentOperation> consumerFactory) {
        this.consumerFactory = consumerFactory;
        createListenerContainer();
    }

    private void createListenerContainer() {
        ContainerProperties containerProperties = new ContainerProperties("input-topic");
        containerProperties.setGroupId("test-group");

        // This is the core fix to ensure handleRecord is called on deserialization failure
        // containerProperties.setCheckDeserExWhenValueNull(false);

        containerProperties.setMessageListener((MessageListener<String, DocumentOperation>) this::handleRecord);

        ConcurrentMessageListenerContainer<String, DocumentOperation> container =
            new ConcurrentMessageListenerContainer<>(consumerFactory, containerProperties);
        container.start();
    }

    private void handleRecord(ConsumerRecord<String, DocumentOperation> data) {
        if (data.value() != null) {
            log.info("Successfully processed record: {}", data.value());
            processedCount.incrementAndGet();
        } else {
            Exception ex = SerializationUtils.getExceptionFromHeader(
                    data,
                    SerializationUtils.VALUE_DESERIALIZER_EXCEPTION_HEADER,
                    new LogAccessor(KafkaInputChannel.class));

            if (ex != null) {
                log.error(">>> Deserialization error caught in handleRecord! Message: {}", ex.getMessage());
                errorCount.incrementAndGet();
            }
        }
    }
}
