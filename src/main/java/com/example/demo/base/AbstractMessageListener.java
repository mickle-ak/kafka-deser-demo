package com.example.demo.base;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.support.serializer.SerializationUtils;

import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@RequiredArgsConstructor
public abstract class AbstractMessageListener implements MessageListener {

    protected final MessageProcessor messageProcessor;
    protected ConcurrentMessageListenerContainer<String, DocumentOperation> container;

    @Getter
    private final AtomicInteger processedCount = new AtomicInteger(0);
    @Getter
    private final AtomicInteger errorCount = new AtomicInteger(0);

    @Override
    public String getImplementationName() {
        return getClass().getSimpleName();
    }


    protected void processRecord(ConsumerRecord<String, DocumentOperation> data) {
        messageProcessor.process(data);
        processedCount.incrementAndGet();
    }

    protected void processDeserializationError(ConsumerRecord<String, DocumentOperation> data) {
        Exception ex = SerializationUtils.getExceptionFromHeader(
                data,
                SerializationUtils.VALUE_DESERIALIZER_EXCEPTION_HEADER,
                new LogAccessor("com.example.demo"));
        messageProcessor.processDeserializationError(data, ex);
        errorCount.incrementAndGet();
    }

    protected abstract ConcurrentMessageListenerContainer<String, DocumentOperation> createListenerContainer();

    @Override
    public void start() {
        if (container == null) {
            container = createListenerContainer();
        }
        if (!container.isRunning()) {
            container.start();
            log.info("Started {}", getImplementationName());
        }
    }

    @Override
    public void stop() {
        if (container != null && container.isRunning()) {
            container.stop();
            log.info("Stopped {}", getImplementationName());
        }
    }
}
