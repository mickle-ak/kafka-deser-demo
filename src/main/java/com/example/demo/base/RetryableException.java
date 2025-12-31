package com.example.demo.base;

import org.springframework.lang.Nullable;

@SuppressWarnings("unused")
public class RetryableException extends RuntimeException {
    public RetryableException() {
        super();
    }

    public RetryableException(String message) {
        super(message);
    }

    public RetryableException(String message, @Nullable Throwable cause) {
        super(message, cause);
    }

    public RetryableException(Throwable cause) {
        super(cause);
    }
}
