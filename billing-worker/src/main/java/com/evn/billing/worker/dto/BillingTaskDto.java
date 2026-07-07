package com.evn.billing.worker.dto;

import java.util.List;

public class BillingTaskDto {
    private String accountId;
    private String bookId;
    private String billingCycleMonth;
    private int period;
    private int calculationVersion;
    private String traceId;
    private List<MeterReadingDto> readings;

    public BillingTaskDto() {
    }

    public BillingTaskDto(String accountId, String bookId, String billingCycleMonth, int period, int calculationVersion, String traceId) {
        this.accountId = accountId;
        this.bookId = bookId;
        this.billingCycleMonth = billingCycleMonth;
        this.period = period;
        this.calculationVersion = calculationVersion;
        this.traceId = traceId;
    }

    public BillingTaskDto(String accountId, String bookId, String billingCycleMonth, int period, int calculationVersion, String traceId, List<MeterReadingDto> readings) {
        this.accountId = accountId;
        this.bookId = bookId;
        this.billingCycleMonth = billingCycleMonth;
        this.period = period;
        this.calculationVersion = calculationVersion;
        this.traceId = traceId;
        this.readings = readings;
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

    public int getPeriod() {
        return period;
    }

    public void setPeriod(int period) {
        this.period = period;
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

    public List<MeterReadingDto> getReadings() {
        return readings;
    }

    public void setReadings(List<MeterReadingDto> readings) {
        this.readings = readings;
    }
}
