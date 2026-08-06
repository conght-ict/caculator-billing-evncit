package com.evn.billing.common.exception;

public class MalformSnapshotException extends RuntimeException {
    public MalformSnapshotException(String message) {
        super(message);
    }

    public MalformSnapshotException(String message, Throwable cause) {
        super(message, cause);
    }
}
