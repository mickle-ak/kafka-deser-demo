package com.example.demo;

import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = {"input-topic"})
@TestPropertySource(properties = {
        "logging.level.org.apache.kafka=WARN",
        "logging.level.kafka=WARN",
        "logging.level.state.change.logger=WARN"
})
class KafkaDeserializationTest {

    @Autowired
    private KafkaInputChannel kafkaInputChannel;

    @Test
    void whenInvalidJsonIsSent_thenHandleRecordIsCalled() {
        Map<String, Object> producerProps = KafkaTestUtils.producerProps(
                System.getProperty("spring.kafka.bootstrap-servers"));

        KafkaTemplate<String, String> rawTemplate = new KafkaTemplate<>(
                new DefaultKafkaProducerFactory<>(producerProps, new StringSerializer(), new StringSerializer())
        );

        // Send valid JSON
        rawTemplate.send("input-topic", "key1", "{\"id\":\"123\", \"type\":\"CREATE\"}");

        // Send INVALID JSON (raw text)
        rawTemplate.send("input-topic", "key2", "NOT_JSON_DATA");

        await().atMost(2, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(kafkaInputChannel.processedCount.get()).isEqualTo(1);
            assertThat(kafkaInputChannel.errorCount.get()).isEqualTo(1);
        });
    }
}
