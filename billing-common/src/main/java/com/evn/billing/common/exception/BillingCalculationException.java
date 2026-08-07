package com.evn.billing.common.exception;

public class BillingCalculationException extends RuntimeException {
    private final String maKhang;
    private final String errorCode;

    public BillingCalculationException(String maKhang, String errorCode, String message) {
        super(message);
        this.maKhang = maKhang;
        this.errorCode = errorCode;
    }

    public BillingCalculationException(String maKhang, String errorCode, String message, Throwable cause) {
        super(message, cause);
        this.maKhang = maKhang;
        this.errorCode = errorCode;
    }

    public String getAccountId() {
        return maKhang;
    }

    public String getErrorCode() {
        return errorCode;
    }
}
