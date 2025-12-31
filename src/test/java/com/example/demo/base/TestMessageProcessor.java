package com.example.demo.base;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Component;

import static org.assertj.core.api.Assertions.assertThat;

@Component
@Slf4j
@RequiredArgsConstructor
public class TestMessageProcessor implements MessageProcessor {

    public static final String KEY_TO_FAIL = "FAIL";
    public static final String KEY_TO_RETRY = "RETRY";
    public static final String INVALID_JSON = "INVALID_JSON";


    private final ProcessingTracker tracker;

    @Override
    public void process(ConsumerRecord<String, DocumentOperation> record) {
        log.info("Processing key={}, value={}", record.key(), record.value());

        // Simulate NotRetryableException for specific key
        if (KEY_TO_FAIL.equals(record.key())) {
            log.info("Throw NotRetryableException for key={}, value={}", record.key(), record.value());
            tracker.recordFailed(record.key());
            throw new NotRetryableException("Simulated non-retryable error for key: " + record.key());
        }

        // Simulate RetryableException for specific key
        if (KEY_TO_RETRY.equals(record.key())) {
            log.info("Throw RetryableException for key={}, value={}", record.key(), record.value());
            tracker.recordRetryAttempt(record.key());
            throw new RetryableException("Simulated retryable error for key: " + record.key());
        }

        // Normal processing
        tracker.recordProcessed(record.key());
    }

    @Override
    public void processDeserializationError(ConsumerRecord<String, DocumentOperation> record, Exception exception) {
        assertThat(exception).isNotNull();
        log.info("Deserialization error for key={}, exception={}", record.key(), exception, exception);
        tracker.recordDeserializationError(record.key());
    }
}
