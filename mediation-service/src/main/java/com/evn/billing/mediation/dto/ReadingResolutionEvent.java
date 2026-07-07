package com.evn.billing.mediation.dto;

import lombok.Data;
import java.math.BigDecimal;

@Data
public class ReadingResolutionEvent {
    private String resolutionType; // ACCEPT_AS_IS, CORRECT
    private String accountId;
    private String billingCycleMonth;
    private String bookId;
    private Long usageId;
    private BigDecimal correctedEndIndex;
}
