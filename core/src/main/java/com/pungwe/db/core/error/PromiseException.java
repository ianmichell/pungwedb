package com.pungwe.db.core.error;

/**
 * Created by ian when 24/06/2016.
 */
public class PromiseException extends DatabaseException {

    private Object wrapped;

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

    public PromiseException(String message, Throwable cause, Object wrapped) {
        super(message, cause);
        this.wrapped = wrapped;
    }

    public PromiseException(Throwable cause, Object wrapped) {
        super(cause);
        this.wrapped = wrapped;
    }

    public PromiseException(String message, Object wrapped) {
        super(message);
        this.wrapped = wrapped;
    }

    public PromiseException(Object wrapped) {
        super();
        this.wrapped = wrapped;
    }

    public Object getWrapped() {
        return wrapped;
    }
}
