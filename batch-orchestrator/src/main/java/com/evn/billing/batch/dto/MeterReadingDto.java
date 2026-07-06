package com.evn.billing.batch.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class MeterReadingDto {
    private String meterPointId;
    private LocalDateTime fromDate;
    private LocalDateTime toDate;
    private BigDecimal startIndex;
    private BigDecimal endIndex;
    private BigDecimal consumption;
}
