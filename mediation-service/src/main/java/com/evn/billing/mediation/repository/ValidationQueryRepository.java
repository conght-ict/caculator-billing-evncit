package com.evn.billing.mediation.repository;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;
import java.util.Map;

public interface ValidationQueryRepository {
    List<Map<String, Object>> findActiveMeterPointsByAccount(String accountId);
    Date findDenNgayByDdoSchedule(String accountId, String month, int period);
    Date findDenNgayByDqlySchedule(String accountId, String month, int period);
    List<Map<String, Object>> findValidatedReadings(String accountId, String month, int period);
    List<String> findValidatedMetersByMeterPoint(String meterPointId, String month, int period);
    List<Map<String, Object>> findNonReplacedReadings(String accountId, String month, int period);
    BigDecimal getCurrentConsumptionSum(String accountId, String month, int period);
    String findMaDviqlyByAccount(String accountId);
}
