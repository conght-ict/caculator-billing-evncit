package com.evn.billing.engine.variant;

import com.evn.billing.common.dto.BillingSchemaStep;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Map;
import java.util.NoSuchElementException;

public class PercentDiscountVariant implements BillingVariant {

    @Override
    public void execute(Map<String, Object> operands, BillingSchemaStep step) throws Exception {
        String amountInKey = step.getInputOperands().get("amount");

        String discountAmountOutKey = step.getOutputOperands().get("discountAmount");
        String netAmountOutKey = step.getOutputOperands().get("amountAfterDiscount");

        BigDecimal baseAmount = (BigDecimal) operands.get(amountInKey);
        if (baseAmount == null) {
            throw new NoSuchElementException("Base amount operand not found: " + amountInKey);
        }

        // Get discount rate from step configuration (default to 0.0)
        double rate = 0.0;
        if (step.getStepConfig() != null && step.getStepConfig().containsKey("discountRate")) {
            Object rateObj = step.getStepConfig().get("discountRate");
            if (rateObj instanceof Number) {
                rate = ((Number) rateObj).doubleValue();
            }
        }

        BigDecimal discountRate = BigDecimal.valueOf(rate);
        BigDecimal discountAmount = baseAmount.multiply(discountRate).setScale(2, RoundingMode.HALF_UP);
        BigDecimal netAmount = baseAmount.subtract(discountAmount).setScale(2, RoundingMode.HALF_UP);

        operands.put(discountAmountOutKey, discountAmount);
        operands.put(netAmountOutKey, netAmount);
    }
}
