package com.evn.billing.mediation.dto;

import lombok.Data;
import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
public class CmisReadingEvent {
    private String accountId;
    private String meterPointId;
    private BigDecimal startIndex;
    private BigDecimal endIndex;
    private String billingCycleMonth; // Format: YYYY_MM_Period (e.g. 2026_06_1)
    private LocalDateTime fromDate;
    private LocalDateTime toDate;
}
