package com.pungwe.db.core.error;

/**
 * Created by ian when 24/06/2016.
 */
public class PromiseException extends DatabaseException {

    public PromiseException() {
    }

    public PromiseException(String message) {
        super(message);
    }

    public PromiseException(String message, Throwable cause) {
        super(message, cause);
    }

    public PromiseException(Throwable cause) {
        super(cause);
    }

    public PromiseException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
