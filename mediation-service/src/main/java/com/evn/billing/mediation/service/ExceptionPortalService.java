package com.evn.billing.mediation.service;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

public interface ExceptionPortalService {
    List<Map<String, Object>> getPendingExceptions(String dtuongQly, String month, int period);
    void resolveException(Long usageId, String month, BigDecimal correctedEndIndex, String operatorNote);
    boolean isBookReadyForBilling(String dtuongQly, String month, int period);
}
