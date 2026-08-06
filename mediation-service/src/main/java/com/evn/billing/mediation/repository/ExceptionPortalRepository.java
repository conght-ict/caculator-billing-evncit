package com.evn.billing.mediation.repository;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

public interface ExceptionPortalRepository {
    List<Map<String, Object>> findPendingExceptions(String dtuongQly, String month, int period);
    void resolveException(Long usageId, String month, BigDecimal correctedEndIndex, String operatorNote);
    int countPendingExceptions(String dtuongQly, String month, int period);
    Map<String, Object> findAccountAndPeriodByUsageId(Long usageId, String month);
}
