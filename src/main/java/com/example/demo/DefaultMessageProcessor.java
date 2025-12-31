package com.example.demo;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Component;

/**
 * Default implementation of MessageProcessor that simply logs the processed message.
 * This is the production implementation used when no custom processor is provided.
 */
@Slf4j
@Component
public class DefaultMessageProcessor implements MessageProcessor {

    @Override
    public void process(ConsumerRecord<String, DocumentOperation> record) {
        log.info("Successfully processed record: {}", record.value());
    }

    @Override
    public void processDeserializationError(ConsumerRecord<String, DocumentOperation> record, Exception exception) {
        log.info("process record with deserialization error: {}", record.value(), exception);
    }
}
