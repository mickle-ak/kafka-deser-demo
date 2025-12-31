package com.example.demo;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration tests for Kafka error handling scenarios.
 * Tests cover: happy path, deserialization errors, retryable exceptions, and non-retryable exceptions.
 */
@Slf4j
class KafkaDeserializationTest {

    private static KafkaTemplate<String, String> createRawTemplate() {
        Map<String, Object> producerProps = KafkaTestUtils.producerProps(
                System.getProperty("spring.kafka.bootstrap-servers"));

        return new KafkaTemplate<>(
                new DefaultKafkaProducerFactory<>(producerProps, new StringSerializer(), new StringSerializer())
        );
    }

    /**
     * Helper class to track processing events during tests
     */
    static class ProcessingTracker {
        final List<String> processedKeys = new CopyOnWriteArrayList<>();
        final List<String> failedKeys = new CopyOnWriteArrayList<>();
        final List<String> deserializationErrorKeys = new CopyOnWriteArrayList<>();
        final Map<String, Integer> retryCount = new ConcurrentHashMap<>();

        void recordProcessed(String key) {
            processedKeys.add(key);
        }

        void recordFailed(String key) {
            failedKeys.add(key);
        }

        void recordDeserializationError(String key) {
            deserializationErrorKeys.add(key);
        }

        void recordRetryAttempt(String key) {
            retryCount.merge(key, 1, Integer::sum);
        }

        void reset() {
            processedKeys.clear();
            failedKeys.clear();
            deserializationErrorKeys.clear();
            retryCount.clear();
        }
    }

    /**
     * Test 1: Happy Path - All messages process successfully
     */
    @Nested
    @SpringBootTest
    @EmbeddedKafka(partitions = 1, topics = {"input-topic"})
    @TestPropertySource(properties = {
            "logging.level.org.apache.kafka=WARN",
            "logging.level.kafka=WARN",
            "logging.level.state.change.logger=WARN"
    })
    class HappyPathTest {

        static final ProcessingTracker tracker = new ProcessingTracker();

        @TestConfiguration
        static class HappyPathTestConfig {
            @Bean
            @Primary
            public MessageProcessor testMessageProcessor() {
                return new MessageProcessor() {
                    @Override
                    public void process(ConsumerRecord<String, DocumentOperation> record) {
                        log.info("Processing key={}, value={}", record.key(), record.value());
                        tracker.recordProcessed(record.key());
                    }

                    @Override
                    public void processDeserializationError(ConsumerRecord<String, DocumentOperation> record, Exception exception) {
                        tracker.deserializationErrorKeys.add(record.key());
                    }
                };
            }
        }

        @Test
        void shouldProcessValidJson_WhenHappyPath() {
            tracker.reset();
            KafkaTemplate<String, String> rawTemplate = createRawTemplate();

            // Send 3 valid JSON messages
            rawTemplate.send("input-topic", "key1", "{\"id\":\"123\", \"type\":\"CREATE\"}");
            rawTemplate.send("input-topic", "key2", "{\"id\":\"456\", \"type\":\"UPDATE\"}");
            rawTemplate.send("input-topic", "key3", "{\"id\":\"789\", \"type\":\"DELETE\"}");

            // All 3 messages should be processed successfully
            await().atMost(3, TimeUnit.SECONDS).untilAsserted(() -> {
                // Verify which keys were processed
                assertThat(tracker.processedKeys).containsExactlyInAnyOrder("key1", "key2", "key3");
                assertThat(tracker.deserializationErrorKeys).isEmpty();
                assertThat(tracker.failedKeys).isEmpty();
                assertThat(tracker.retryCount).isEmpty();
            });
        }
    }

    /**
     * Test 2: Deserialization Error - Invalid JSON results in null value
     */
    @Nested
    @SpringBootTest
    @EmbeddedKafka(partitions = 1, topics = {"input-topic"})
    @TestPropertySource(properties = {
            "logging.level.org.apache.kafka=WARN",
            "logging.level.kafka=WARN",
            "logging.level.state.change.logger=WARN"
    })
    class DeserializationErrorTest {

        static final ProcessingTracker tracker = new ProcessingTracker();

        @TestConfiguration
        static class DeserializationErrorTestConfig {
            @Bean
            @Primary
            public MessageProcessor testMessageProcessor() {
                return new MessageProcessor() {
                    @Override
                    public void process(ConsumerRecord<String, DocumentOperation> record) {
                        log.info("Processing key={}, value={}", record.key(), record.value());
                        tracker.recordProcessed(record.key());
                    }

                    @Override
                    public void processDeserializationError(ConsumerRecord<String, DocumentOperation> record, Exception exception) {
                        log.info("Deserialization error for key={}, exception={}", record.key(),
                                exception != null ? exception.getMessage() : "null");
                        tracker.recordDeserializationError(record.key());
                    }
                };
            }
        }

        @Test
        void shouldHandleNullValue_WhenDeserializationError() {
            tracker.reset();
            KafkaTemplate<String, String> rawTemplate = createRawTemplate();

            // Send valid JSON
            rawTemplate.send("input-topic", "key1", "{\"id\":\"123\", \"type\":\"CREATE\"}");

            // Send INVALID JSON (raw text) - will result in null value
            rawTemplate.send("input-topic", "key2", "NOT_JSON_DATA");

            // Send another valid JSON - should still be processed
            rawTemplate.send("input-topic", "key3", "{\"id\":\"456\", \"type\":\"UPDATE\"}");

            // 2 successful, 1 deserialization error
            await().atMost(3, TimeUnit.SECONDS).untilAsserted(() -> {
                // Verify only key1 and key3 were processed (key2 had deserialization error)
                assertThat(tracker.processedKeys).containsExactlyInAnyOrder("key1", "key3");
                assertThat(tracker.deserializationErrorKeys).containsExactly("key2");
                assertThat(tracker.failedKeys).isEmpty();
                assertThat(tracker.retryCount).isEmpty();
            });
        }
    }

    /**
     * Test 3: NotRetryableException - Skip failed record, process following records
     */
    @Nested
    @SpringBootTest
    @EmbeddedKafka(partitions = 1, topics = {"input-topic"})
    @TestPropertySource(properties = {
            "logging.level.org.apache.kafka=WARN",
            "logging.level.kafka=WARN",
            "logging.level.state.change.logger=WARN"
    })
    class NotRetryableExceptionTest {

        static final ProcessingTracker tracker = new ProcessingTracker();

        /**
         * Test configuration that injects a MessageProcessor which throws
         * NotRetryableException for specific keys.
         */
        @TestConfiguration
        static class NotRetryableExceptionTestConfig {
            @Bean
            @Primary
            public MessageProcessor testMessageProcessor() {
                return new MessageProcessor() {
                    @Override
                    public void process(ConsumerRecord<String, DocumentOperation> record) {
                        log.info("Processing key={}, value={}", record.key(), record.value());

                        // Throw NotRetryableException for key2
                        if ("key2".equals(record.key())) {
                            tracker.recordFailed(record.key());
                            throw new ErrorHandlerConfig.NotRetryableException(
                                    "Simulated non-retryable error for key: " + record.key());
                        }
                        // Other records process normally
                        tracker.recordProcessed(record.key());
                    }

                    @Override
                    public void processDeserializationError(ConsumerRecord<String, DocumentOperation> record, Exception exception) {
                        tracker.deserializationErrorKeys.add(record.key());
                    }
                };
            }
        }

        @Test
        void shouldSkipRecord_WhenNotRetryableException_AndProcessFollowingRecords() {
            tracker.reset();
            KafkaTemplate<String, String> rawTemplate = createRawTemplate();

            // Send 3 valid JSON messages, but key2 will trigger NotRetryableException
            rawTemplate.send("input-topic", "key1", "{\"id\":\"123\", \"type\":\"CREATE\"}");
            rawTemplate.send("input-topic", "key2", "{\"id\":\"456\", \"type\":\"UPDATE\"}"); // Will fail with NotRetryableException
            rawTemplate.send("input-topic", "key3", "{\"id\":\"789\", \"type\":\"DELETE\"}");

            // key1 and key3 should be processed (2 total)
            // key2 should fail but NOT be retried and subsequent messages should still process
            await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
                // Verify which keys were processed vs failed
                assertThat(tracker.processedKeys).containsExactlyInAnyOrder("key1", "key3");
                assertThat(tracker.failedKeys).containsExactly("key2");
                assertThat(tracker.deserializationErrorKeys).isEmpty();
                assertThat(tracker.retryCount).isEmpty(); // No retries for NotRetryableException
            });
        }
    }

    /**
     * Test 4: RetryableException - Retry failed record, then skip, process following records
     */
    @Nested
    @SpringBootTest
    @EmbeddedKafka(partitions = 1, topics = {"input-topic"})
    @TestPropertySource(properties = {
            "logging.level.org.apache.kafka=WARN",
            "logging.level.kafka=WARN",
            "logging.level.state.change.logger=WARN",
            "logging.level.com.example.demo=INFO"  // Enable INFO logs to see retry attempts
    })
    class RetryableExceptionTest {

        static final ProcessingTracker tracker = new ProcessingTracker();

        /**
         * Test configuration that injects a MessageProcessor which throws
         * RetryableException for specific keys.
         */
        @TestConfiguration
        static class RetryableExceptionTestConfig {
            @Bean
            @Primary
            public MessageProcessor testMessageProcessor() {
                return new MessageProcessor() {
                    @Override
                    public void process(ConsumerRecord<String, DocumentOperation> record) {
                        log.info("Processing key={}, value={}", record.key(), record.value());

                        // Throw RetryableException for key2 (will be retried 3 times)
                        if ("key2".equals(record.key())) {
                            tracker.recordRetryAttempt(record.key());
                            throw new ErrorHandlerConfig.RetryableException(
                                    "Simulated retryable error for key: " + record.key());
                        }
                        // Other records process normally
                        tracker.recordProcessed(record.key());
                    }

                    @Override
                    public void processDeserializationError(ConsumerRecord<String, DocumentOperation> record, Exception exception) {
                        tracker.deserializationErrorKeys.add(record.key());
                    }
                };
            }
        }

        @Test
        void shouldRetryThenSkip_WhenRetryableException_AndProcessFollowingRecords() {
            tracker.reset();
            KafkaTemplate<String, String> rawTemplate = createRawTemplate();

            // Send 3 valid JSON messages, but key2 will trigger RetryableException
            rawTemplate.send("input-topic", "key1", "{\"id\":\"123\", \"type\":\"CREATE\"}");
            rawTemplate.send("input-topic", "key2", "{\"id\":\"456\", \"type\":\"UPDATE\"}"); // Will fail with RetryableException and be retried
            rawTemplate.send("input-topic", "key3", "{\"id\":\"789\", \"type\":\"DELETE\"}");

            // key1 and key3 should be processed (2 total)
            // key2 should be retried 3 times (with 500ms backoff), then sent to recoverer, and key3 should still process
            await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
                // Verify which keys were processed
                assertThat(tracker.processedKeys).containsExactlyInAnyOrder("key1", "key3");
                assertThat(tracker.retryCount).containsEntry("key2", 3); // 3 attempts
                assertThat(tracker.deserializationErrorKeys).isEmpty();
                assertThat(tracker.failedKeys).isEmpty(); // RetryableException doesn't record as "failed" in our tracker
            });
        }
    }
}
