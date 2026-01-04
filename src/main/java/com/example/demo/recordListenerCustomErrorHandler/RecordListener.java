package com.example.demo.recordListenerCustomErrorHandler;

import com.example.demo.base.AbstractMessageListener;
import com.example.demo.base.DocumentOperation;
import com.example.demo.base.MessageProcessor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.stereotype.Component;


@Slf4j
@Profile("recordListenerCustomErrorHandler")
@Component
public class RecordListener extends AbstractMessageListener {

    private final ConsumerFactory<String, DocumentOperation> consumerFactory;
    private final CommonErrorHandler errorHandler;

    public RecordListener(ConsumerFactory<String, DocumentOperation> consumerFactory,
                          CommonErrorHandler errorHandler,
                          MessageProcessor messageProcessor) {
        super(messageProcessor);
        this.consumerFactory = consumerFactory;
        this.errorHandler = errorHandler;
        start();
    }


    protected ConcurrentMessageListenerContainer<String, DocumentOperation> createListenerContainer() {
        ContainerProperties containerProperties = new ContainerProperties("input-topic");
        containerProperties.setGroupId("test-group");

        // This is the core fix to ensure handleRecord is called on deserialization failure
        containerProperties.setCheckDeserExWhenValueNull(false);

        containerProperties.setMessageListener((MessageListener<String, DocumentOperation>) this::handleRecord);

        var newContainer = new ConcurrentMessageListenerContainer<>(consumerFactory, containerProperties);
        newContainer.setCommonErrorHandler(errorHandler);
        return newContainer;
    }

    private void handleRecord(ConsumerRecord<String, DocumentOperation> data) {
        if (data.value() != null) {
            processRecord(data);
        } else {
            processDeserializationError(data);
        }
    }


    @Override
    protected void processDeserializationError(ConsumerRecord<String, DocumentOperation> data) {
        processDeserializationError(data, KafkaDeserializationErrorHandler.CUSTOM_DESERIALIZATION_ERROR_HEADER);
    }
}
