package com.evn.billing.common.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.io.Serializable;
import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class BillingSchemaStep implements Serializable {
    private static final long serialVersionUID = 1L;

    private int stepNumber;
    private String variantName;                 // e.g., "STEP_RATING", "PERCENT_DISCOUNT", "TAX"
    private Map<String, String> inputOperands;  // e.g., {"consumption": "NET_KWH", "tariffCode": "FAST_TARIFF_CODE"}
    private Map<String, String> outputOperands; // e.g., {"amount": "BASE_AMOUNT", "steps": "RATING_STEPS"}
    private Map<String, Object> stepConfig;     // e.g., {"discountRate": 0.10} or other configurations
}
