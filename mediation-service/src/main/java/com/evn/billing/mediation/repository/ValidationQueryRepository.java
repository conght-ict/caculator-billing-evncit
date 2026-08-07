package com.evn.billing.mediation.repository;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;
import java.util.Map;

public interface ValidationQueryRepository {
    List<Map<String, Object>> findActiveMeterPointsByAccount(String maKhang);
    Date findDenNgayByDdoSchedule(String maKhang, String month, int period);
    Date findDenNgayByDqlySchedule(String maKhang, String month, int period);
    List<Map<String, Object>> findValidatedReadings(String maKhang, String month, int period);
    List<String> findValidatedMetersByMeterPoint(String meterPointId, String month, int period);
    List<Map<String, Object>> findNonReplacedReadings(String maKhang, String month, int period);
    BigDecimal getCurrentConsumptionSum(String maKhang, String month, int period);
    String findMaDviqlyByAccount(String maKhang);
}
