package com.evn.billing.mediation.repository;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface MonitoringQueryRepository {
    List<Map<String, Object>> findBooks();
    void initPendingStatuses(String month, int period, String dtuongQly);
    List<Map<String, Object>> findAccountsWithStatus(String month, int period, String dtuongQly);
    List<Map<String, Object>> findCalculationLogs(String dtuongQly, String maKhang, String status, int limit);
    Optional<Map<String, Object>> findCalculationLogDetail(String logId);
    List<Map<String, Object>> findErrorLogs(String maKhang, String month, Integer period, int limit);
    List<Map<String, Object>> findBatchExecutions();
    List<Map<String, Object>> findBookBillingRuns();
    List<Map<String, Object>> findBatchStepExecutions(Long jobExecutionId);
    List<Long> findLatestJobExecutionIds(String dtuongQly, String month);
    Optional<Map<String, Object>> findLatestActiveScheduleByBook(String dtuongQly);
    String findBookByAccountId(String maKhang);
}
