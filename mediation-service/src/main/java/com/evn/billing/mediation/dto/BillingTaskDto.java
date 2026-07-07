package com.evn.billing.mediation.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class BillingTaskDto {
    private String accountId;
    private String bookId;
    private String billingCycleMonth;
    private int period;
    private int calculationVersion;
    private String traceId;
    private List<MeterReadingDto> readings;
}
