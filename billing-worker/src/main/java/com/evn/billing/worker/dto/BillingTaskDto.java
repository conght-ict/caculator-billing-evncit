package com.evn.billing.worker.dto;

public class BillingTaskDto {
    private String accountId;
    private String bookId;
    private String billingCycleMonth;
    private int calculationVersion;
    private String traceId;

    public BillingTaskDto() {
    }

    public BillingTaskDto(String accountId, String bookId, String billingCycleMonth, int calculationVersion, String traceId) {
        this.accountId = accountId;
        this.bookId = bookId;
        this.billingCycleMonth = billingCycleMonth;
        this.calculationVersion = calculationVersion;
        this.traceId = traceId;
    }

    public String getAccountId() {
        return accountId;
    }

    public void setAccountId(String accountId) {
        this.accountId = accountId;
    }

    public String getBookId() {
        return bookId;
    }

    public void setBookId(String bookId) {
        this.bookId = bookId;
    }

    public String getBillingCycleMonth() {
        return billingCycleMonth;
    }

    public void setBillingCycleMonth(String billingCycleMonth) {
        this.billingCycleMonth = billingCycleMonth;
    }

    public int getCalculationVersion() {
        return calculationVersion;
    }

    public void setCalculationVersion(int calculationVersion) {
        this.calculationVersion = calculationVersion;
    }

    public String getTraceId() {
        return traceId;
    }

    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }
}
