package com.evn.billing.common.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.io.Serializable;
import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class RetryTaskDto implements Serializable {
    private static final long serialVersionUID = 1L;

    private String originalTaskId;
    private String maKhang;
    private String billingCycleMonth;
    private int period;
    private int retryCount;
    private String lastError;
    private LocalDateTime nextRetryAt;
}
