package com.evn.billing.engine;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CalculationResult {
    private BigDecimal totalAmountBeforeTax;
    private BigDecimal taxAmount;
    private BigDecimal totalAmountAfterTax;
    private BigDecimal discountAmount;
    
    private Map<String, Object> meterPointBreakdowns;
    private List<Map<String, Object>> stepDetails;
    private Map<String, BigDecimal> nodeNetConsumptions;
}
