package com.pungwe.db.core.error;

/**
 * Created by ian on 24/06/2016.
 */
public class IndexAlreadyExistsException extends DatabaseException {

    public IndexAlreadyExistsException() {
    }

    public IndexAlreadyExistsException(String message) {
        super(message);
    }

    public IndexAlreadyExistsException(String message, Throwable cause) {
        super(message, cause);
    }

    public IndexAlreadyExistsException(Throwable cause) {
        super(cause);
    }

    public IndexAlreadyExistsException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

}
