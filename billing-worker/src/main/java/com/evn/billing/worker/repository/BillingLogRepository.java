package com.evn.billing.worker.repository;

import com.evn.billing.worker.service.BillingLogService;

import java.util.List;

public interface BillingLogRepository {
    void batchInsertCalculationLogs(List<BillingLogService.CalculationLogEntry> entries);
}
