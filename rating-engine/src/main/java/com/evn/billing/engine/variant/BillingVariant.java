package com.evn.billing.engine.variant;

import com.evn.billing.common.dto.BillingSchemaStep;
import java.util.Map;

public interface BillingVariant {
    /**
     * Executes the specific billing variant logic.
     * 
     * @param operands The context containing intermediate values (inputs/outputs)
     * @param step The schema step configuration details
     */
    void execute(Map<String, Object> operands, BillingSchemaStep step) throws Exception;
}
