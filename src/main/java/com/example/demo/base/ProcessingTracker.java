package com.example.demo.base;

import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Helper class to track processing events during tests
 */
@Component
public class ProcessingTracker {

    public final Collection<String> processedKeys = new ConcurrentLinkedQueue<>();
    public final Collection<String> failedKeys = new ConcurrentLinkedQueue<>();
    public final Collection<String> deserializationErrorKeys = new ConcurrentLinkedQueue<>();
    public final Map<String, Integer> retryCount = new ConcurrentHashMap<>();

    public void recordProcessed(String key) {
        processedKeys.add(key);
    }

    public void recordFailed(String key) {
        failedKeys.add(key);
    }

    public void recordDeserializationError(String key) {
        deserializationErrorKeys.add(key);
    }

    public void recordRetryAttempt(String key) {
        retryCount.merge(key, 1, Integer::sum);
    }


    public void reset() {
        processedKeys.clear();
        failedKeys.clear();
        deserializationErrorKeys.clear();
        retryCount.clear();
    }
}
