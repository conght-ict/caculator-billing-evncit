package com.evn.billing.mediation.repository;

import java.util.List;
import java.util.Map;

public interface AmrIngestionRepository {
    void migrateSchema();
    List<Map<String, Object>> findActivePendingSchedules();
    List<Map<String, Object>> findActiveMetersByDtuongQly(String dtuongQly);
    List<Map<String, Object>> findIngestedReadings(String month, int period);
    List<Map<String, Object>> findOracleReadings(List<String> meterIds, java.time.LocalDateTime start, java.time.LocalDateTime end);
    void updateScheduleStatus(String dtuongQly, String month, int period, String status);
    void batchInsertAmrReadings(List<Object[]> params);
    
    String findDtuongQlyByAccountId(String accountId);
    List<Map<String, Object>> findScheduleTolerance(String dtuongQly, String month, int period);
    void batchInsertCmisReadings(List<Object[]> params);
    
    boolean tryAcquireBillingTriggerGate(String dtuongQly, String accountId, String month, int period);
    void logIncompleteStatus(String accountId, String month, String dtuongQly, int period, String missingStr);
    
    List<Map<String, Object>> getKh2tpPmaxStatus(String accountId, String month, int period);
    List<Map<String, Object>> getReactivePowerStatus(String accountId, String month, int period);
    java.math.BigDecimal getPreviousPeriodConsumption(String accountId, String currentMonth, int currentPeriod);
    List<java.math.BigDecimal> getHistoricalConsumptions(String accountId, String currentMonth, int currentPeriod);
    void updateCustomerBillingStatus(String accountId, String month, int period, String status, String errorMsg);
    boolean isBatchJobRunning(String dtuongQly, String month, int period);
    void logIngestionLifecycle(String accountId, String meterPointId, String month, Integer period, String step, String status, String detailJson, String source);
}
