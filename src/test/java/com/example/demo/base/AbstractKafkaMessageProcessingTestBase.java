package com.example.demo.base;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.TestPropertySource;

import java.util.concurrent.TimeUnit;

import static com.example.demo.base.TestMessageProcessor.*;
import static org.assertj.core.api.SoftAssertions.assertSoftly;
import static org.awaitility.Awaitility.await;

/**
 * Abstract base class for testing different Kafka listener implementations.
 * <p>
 * All concrete test classes should extend this class and provide:
 * <ul>
 *   <li>either @TestConfiguration that creates the specific listener implementation</li>
 *   <li>or set correct active profile to allow spring to create the specific listener implementation</li>
 * </ul>
 *
 * <p>This approach allows testing different listener implementations with the same test suite.
 */
@Slf4j
@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = {"input-topic"})
@TestPropertySource(properties = {
        "logging.level.org.apache.kafka=WARN",
        "logging.level.kafka=WARN",
        "logging.level.state.change.logger=WARN",
        "logging.level.com.example.demo=INFO"
})
public abstract class AbstractKafkaMessageProcessingTestBase {

    @Autowired
    protected MessageListener listener;

    @Autowired
    protected ProcessingTracker tracker;

    @Autowired
    private KafkaTemplate<String, String> rawTemplate;


    @Test
    void shouldProcess_ValidJson_InvalidJson_NotRetryableException_RetryableException() {
        log.info("Start test for implementation: {}", listener.getImplementationName());

        // Send INVALID JSON (raw text) - will result in null value
        rawTemplate.send("input-topic", INVALID_JSON, "NOT_JSON_DATA");
        // Send valid JSON messages, which should be processed correct and only once
        rawTemplate.send("input-topic", "key1", "{\"id\":\"123\", \"type\":\"CREATE\"}");
        // Send key to trigger NotRetryableException
        rawTemplate.send("input-topic", KEY_TO_FAIL, "{\"id\":\"456\", \"type\":\"UPDATE\"}"); // Will fail with NotRetryableException
        // Send key to trigger RetryableException
        rawTemplate.send("input-topic", KEY_TO_RETRY, "{\"id\":\"456\", \"type\":\"UPDATE\"}"); // Will fail with RetryableException and be retried
        // Send another valid JSON messages, which should be processed correct and only once
        rawTemplate.send("input-topic", "key2", "{\"id\":\"123\", \"type\":\"CREATE\"}");

        // All 3 messages should be processed successfully
        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> assertSoftly(s -> {
            // Verify which keys were processed
            s.assertThat(tracker.processedKeys).containsExactlyInAnyOrder("key1", "key2");
            s.assertThat(tracker.deserializationErrorKeys).containsExactly(INVALID_JSON);
            s.assertThat(tracker.failedKeys).containsExactly(KEY_TO_FAIL);
            s.assertThat(tracker.retryCount).containsEntry(KEY_TO_RETRY, 3); // 3 attempts
        }));

        log.info("Test passed for implementation: {}", listener.getImplementationName());
    }
}
