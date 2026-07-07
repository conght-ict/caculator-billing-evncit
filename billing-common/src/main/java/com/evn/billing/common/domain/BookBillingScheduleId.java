package com.evn.billing.common.domain;

import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class BookBillingScheduleId implements Serializable {
    private String bookId;
    private String billingCycleMonth;
    private Integer period;
}
