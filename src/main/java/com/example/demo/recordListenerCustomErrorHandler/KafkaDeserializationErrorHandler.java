package com.example.demo.recordListenerCustomErrorHandler;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.kafka.support.serializer.FailedDeserializationInfo;
import org.springframework.kafka.support.serializer.SerializationUtils;
import org.springframework.util.Assert;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;


@Slf4j
public class KafkaDeserializationErrorHandler {

    public static final String CUSTOM_DESERIALIZATION_ERROR_HEADER = "custom-deserialization-error-header";

    public static <T> T onDeserializationError(FailedDeserializationInfo dfi) {
        Headers headers = dfi.getHeaders();
        Header exceptionHeader = headers.lastHeader(SerializationUtils.VALUE_DESERIALIZER_EXCEPTION_HEADER);
        if (exceptionHeader != null) {
            headers.remove(SerializationUtils.VALUE_DESERIALIZER_EXCEPTION_HEADER);
            headers.add(CUSTOM_DESERIALIZATION_ERROR_HEADER, exceptionHeader.value());
        }
        return null;
    }

    public static DeserializationException getDeserializationException(ConsumerRecord<?, ?> record, String headerName) {
        Header header = record.headers().lastHeader(headerName);
        if (header != null) {
            DeserializationException exception = byteArrayToDeserializationException(header);
            if (exception != null) {
                Headers headers = new RecordHeaders(record.headers().toArray());
                headers.remove(headerName);
                exception.setHeaders(headers);
            }
            return exception;
        }
        return null;
    }

    private static DeserializationException byteArrayToDeserializationException(Header header) {
        try(ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(header.value()))) {
            return (DeserializationException) ois.readObject();
        }
        catch (IOException | ClassNotFoundException | ClassCastException e) {
            log.error("Failed to deserialize a deserialization exception", e);
            return null;
        }
    }
}
