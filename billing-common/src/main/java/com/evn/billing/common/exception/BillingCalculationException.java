package com.evn.billing.common.exception;

public class BillingCalculationException extends RuntimeException {
    private final String accountId;
    private final String errorCode;

    public BillingCalculationException(String accountId, String errorCode, String message) {
        super(message);
        this.accountId = accountId;
        this.errorCode = errorCode;
    }

    public BillingCalculationException(String accountId, String errorCode, String message, Throwable cause) {
        super(message, cause);
        this.accountId = accountId;
        this.errorCode = errorCode;
    }

    public String getAccountId() {
        return accountId;
    }

    public String getErrorCode() {
        return errorCode;
    }
}
