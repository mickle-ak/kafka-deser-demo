package com.example.demo.batchListenerManuelRetry;

import com.example.demo.base.NotRetryableException;
import com.example.demo.base.RetryableException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.BackOff;

import java.util.HashSet;
import java.util.Set;

@Slf4j
@Configuration
@Profile("batchListenerManuelRetry")
public class ErrorHandlerConfig {

    /**
     * Configures and provides a {@link CommonErrorHandler} for pipeline processing consumers.
     * This error handler uses a specified {@link ConsumerRecordRecoverer} and {@link BackOff} strategy,
     * and defines which exceptions are retryable or not.
     *
     * @return A configured {@link DefaultErrorHandler} instance.
     */
    @Bean
    public CommonErrorHandler errorHandler() {
        DefaultErrorHandler errorHandler = new DefaultErrorHandler();
        // exception processing polices
        errorHandler.addRetryableExceptions(RetryableException.class);
        errorHandler.addNotRetryableExceptions(NotRetryableException.class);
        errorHandler.addNotRetryableExceptions(IllegalArgumentException.class);
        errorHandler.setAckAfterHandle(true);  // it is important in manual acknowledge modes, because explicit set
        errorHandler.setCommitRecovered(true); // it is important in manual acknowledge modes, because explicit set
        // logging
        errorHandler.setLogLevel(KafkaException.Level.ERROR);
        return errorHandler;
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
