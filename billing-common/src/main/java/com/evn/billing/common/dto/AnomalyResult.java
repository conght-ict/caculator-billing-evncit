package com.evn.billing.common.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.io.Serializable;
import java.math.BigDecimal;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AnomalyResult implements Serializable {
    private static final long serialVersionUID = 1L;

    private boolean isAnomaly;
    private BigDecimal zScore;
    private BigDecimal ema;
    private BigDecimal stdDev;
}
