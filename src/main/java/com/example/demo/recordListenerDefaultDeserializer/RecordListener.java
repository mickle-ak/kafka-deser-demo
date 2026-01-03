package com.example.demo.recordListenerDefaultDeserializer;

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
@Profile("recordListenerDefaultDeserializer")
@Component
public class RecordListener extends AbstractMessageListener {

    private final ConsumerFactory<String, DocumentOperation> consumerFactory;
    private final CommonErrorHandler errorHandler;

    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
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

        containerProperties.setMessageListener((MessageListener<String, DocumentOperation>) this::handleRecord);

        var listenerContainer = new ConcurrentMessageListenerContainer<>(consumerFactory, containerProperties);
        listenerContainer.setCommonErrorHandler(errorHandler);
        return listenerContainer;
    }

    private void handleRecord(ConsumerRecord<String, DocumentOperation> data) {
        if (data.value() != null) {
            processRecord(data);
        } else {
            processDeserializationError(data);
        }
    }
}
