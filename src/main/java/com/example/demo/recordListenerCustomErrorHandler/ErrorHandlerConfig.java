package com.example.demo.recordListenerCustomErrorHandler;

import com.example.demo.base.NotRetryableException;
import com.example.demo.base.RetryableException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.listener.*;
import org.springframework.lang.NonNull;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Slf4j
@Configuration
@Profile("recordListenerCustomErrorHandler")
public class ErrorHandlerConfig {
    /**
     * Configures and provides a {@link CommonErrorHandler} for pipeline processing consumers.
     * This error handler uses a specified {@link ConsumerRecordRecoverer} and {@link BackOff} strategy,
     * and defines which exceptions are retryable or not.
     *
     * @param recoverer      The {@link ConsumerRecordRecoverer} to use for recovering from failed messages.
     * @param fixedBackOff   The {@link BackOff} strategy to apply for retries.
     * @param retryListeners A list of {@link RetryListener} instances to be notified during retry attempts.
     * @return A configured {@link DefaultErrorHandler} instance.
     * @see #consumerRecordRecoverer
     * @see #retryStrategy
     * @see #loggingRetryListener
     */
    @Bean
    public CommonErrorHandler errorHandler(
            ConsumerRecordRecoverer recoverer,
            BackOff fixedBackOff,
            List<RetryListener> retryListeners
    ) {
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, fixedBackOff);
        // exception processing polices
        errorHandler.addRetryableExceptions(RetryableException.class);
        errorHandler.addNotRetryableExceptions(NotRetryableException.class);
        errorHandler.addNotRetryableExceptions(IllegalArgumentException.class);
        errorHandler.setAckAfterHandle(true);  // it is important in manual acknowledge modes, because explicit set
        errorHandler.setCommitRecovered(true); // it is important in manual acknowledge modes, because explicit set
        // logging
        errorHandler.setLogLevel(KafkaException.Level.ERROR);
        if (!retryListeners.isEmpty()) {
            errorHandler.setRetryListeners(retryListeners.toArray(new RetryListener[0]));
        }
        return errorHandler;
    }

    /**
     * Defines the retry strategy for pipeline processing.
     * <p>
     * This should be configurable, ideally fetched from application properties.
     *
     * @return A {@link FixedBackOff} instance representing the retry strategy.
     */
    @Bean
    public BackOff retryStrategy() {
        return new FixedBackOff(500, 2);  // to get 3 executions (1+2 retry)
    }

    /**
     * Provides a {@link ConsumerRecordRecoverer} for handling records that have failed all retry attempts.
     * This implementation uses a {@link LoggingOnlyRecoverer}, which simply logs the failed record.
     * <p>
     * WARNING: For production environments, it is highly recommended to use a {@link DeadLetterPublishingRecoverer}
     * to send failed messages to a Dead Letter Queue (DLQ) for further investigation and reprocessing.
     *
     * @return An instance of {@link LoggingOnlyRecoverer}.
     */
    @Bean
    public ConsumerRecordRecoverer consumerRecordRecoverer() {
        return new LoggingOnlyRecoverer();
    }

    /**
     * A simple {@link ConsumerRecordRecoverer} that only logs the failed consumer record
     * after all retry attempts have been exhausted.
     * <p>
     * NOTE: This is a placeholder for demonstration purposes. In a production system,
     * a {@link DeadLetterPublishingRecoverer} or similar mechanism for handling dead letters should be implemented.
     */
    private static class LoggingOnlyRecoverer implements ConsumerRecordRecoverer {

        /**
         * Accepts a failed consumer record and the exception that caused the failure, logging the details.
         *
         * @param cr The consumer record that failed processing.
         * @param e  The exception that caused the processing to fail.
         */
        @Override
        public void accept(ConsumerRecord<?, ?> cr, Exception e) {
            log.error("""
                              
                              ***
                              *** Message processing failed after all retries. Sending to nowhere (just logging for now).
                              *** Record: [{}-{}@{}] Key: '{}' - auto commit after recovery
                              *** Error: {}
                              *** Value:
                              {}
                              ***
                              """,
                      cr.topic(), cr.partition(), cr.offset(), cr.key(),
                      exceptionAsString(e),
                      cr.value());
        }
    }


    /**
     * Provides a {@link RetryListener} for logging retry attempts.
     *
     * @return An instance of {@link LoggingRetryListener}.
     */
    @Bean
    public RetryListener loggingRetryListener() {
        return new LoggingRetryListener();
    }

    /**
     * A simple {@link RetryListener} that logs a warning message whenever
     * a message delivery fails and a retry attempt is initiated.
     */
    private static class LoggingRetryListener implements RetryListener {
        @Override
        public void failedDelivery(ConsumerRecord<?, ?> cr, @NonNull Exception ex, int deliveryAttempt) {
            log.warn("Retrying ({} attempt) for key '{}': \n\t{}", deliveryAttempt, cr.key(), exceptionAsString(ex));
        }
    }


    //-------------------------------
    //---                         ---
    //---        helpers          ---
    //---                         ---
    //-------------------------------

    /**
     * Returns a formatted string representation of the exception chain.
     * This method traverses the cause chain of an exception and presents it in a readable format.
     *
     * @param e the exception to format
     * @return a formatted string including class names and messages of all causes
     */
    public static String exceptionAsString(Throwable e) {
        Set<Throwable> processed = new HashSet<>();
        return exceptionAsString(e, processed);
    }

    private static String exceptionAsString(Throwable e, Set<Throwable> processed) {
        if (!processed.add(e)) return ""; // already processed
        return "%s: %s%s".formatted(e.getClass().getSimpleName(), e.getMessage(),
                                    (e.getCause() != null
                                            ? "%n\t\tCaused by: %s ".formatted(exceptionAsString(e.getCause(), processed))
                                            : ""));
    }
}
