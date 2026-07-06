package com.evn.billing.engine.variant;

import com.evn.billing.common.dto.BillingSchemaStep;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Map;
import java.util.NoSuchElementException;

public class TaxCalculationVariant implements BillingVariant {

    @Override
    public void execute(Map<String, Object> operands, BillingSchemaStep step) throws Exception {
        String amountInKey = step.getInputOperands().get("amount");

        String taxAmountOutKey = step.getOutputOperands().get("taxAmount");
        String totalAmountOutKey = step.getOutputOperands().get("totalAmount");

        BigDecimal taxableAmount = (BigDecimal) operands.get(amountInKey);
        if (taxableAmount == null) {
            throw new NoSuchElementException("Taxable amount operand not found: " + amountInKey);
        }

        // Get tax rate from step configuration (default to 10% / 0.10)
        double rate = 0.10;
        if (step.getStepConfig() != null && step.getStepConfig().containsKey("taxRate")) {
            Object rateObj = step.getStepConfig().get("taxRate");
            if (rateObj instanceof Number) {
                rate = ((Number) rateObj).doubleValue();
            }
        }

        BigDecimal taxRate = BigDecimal.valueOf(rate);
        BigDecimal taxAmount = taxableAmount.multiply(taxRate).setScale(2, RoundingMode.HALF_UP);
        BigDecimal totalAmount = taxableAmount.add(taxAmount).setScale(2, RoundingMode.HALF_UP);

        operands.put(taxAmountOutKey, taxAmount);
        operands.put(totalAmountOutKey, totalAmount);
    }
}
