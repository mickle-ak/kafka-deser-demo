package com.example.demo.base;

import org.springframework.lang.Nullable;

@SuppressWarnings("unused")
public class NotRetryableException extends RuntimeException {
    public NotRetryableException() {
        super();
    }

    public NotRetryableException(String message) {
        super(message);
    }

    public NotRetryableException(String message, @Nullable Throwable cause) {
        super(message, cause);
    }

    public NotRetryableException(Throwable cause) {
        super(cause);
    }
}
