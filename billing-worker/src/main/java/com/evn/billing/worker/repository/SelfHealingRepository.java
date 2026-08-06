package com.evn.billing.worker.repository;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

public interface SelfHealingRepository {
    void insertErrorLog(String accountId, String month, int period, String errorType, String errorDetails);
    void insertDlqTask(String accountId, String month, int period, int retryCount, String errorMessage, Timestamp nextRetryAt);
    List<Map<String, Object>> findPendingRetryTasks(int limit);
    void markRetryTaskCompleted(Long taskId);
    void markRetryTaskFailed(Long taskId, String note);
    String findBookFromBillingStatus(String accountId, String month, int period);
    String findBookByAccountId(String accountId);
}
