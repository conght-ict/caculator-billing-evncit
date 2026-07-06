package com.evn.billing.mediation.dto;

import lombok.Data;
import java.math.BigDecimal;

@Data
public class CmisReadingEvent {
    private String accountId;
    private String meterPointId;
    private BigDecimal startIndex;
    private BigDecimal endIndex;
    private String billingCycleMonth;
}
