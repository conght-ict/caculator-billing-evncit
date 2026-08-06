package com.evn.billing.engine.variant;

import com.evn.billing.common.dto.BillingSchemaStep;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Map;
import java.util.NoSuchElementException;

public class TaxCalculationVariant implements BillingVariant {

    @Override
    public void execute(Map<String, Object> operands, BillingSchemaStep step) throws Exception {
        if (step.getInputOperands() == null) {
            throw new IllegalArgumentException("Input operands are not configured in TAX schema step");
        }
        if (step.getOutputOperands() == null) {
            throw new IllegalArgumentException("Output operands are not configured in TAX schema step");
        }

        String amountInKey = step.getInputOperands().get("amount");
        if (amountInKey == null) {
            throw new IllegalArgumentException("Input operand key for 'amount' is not configured in TAX schema step");
        }

        String taxAmountOutKey = step.getOutputOperands().get("taxAmount");
        String totalAmountOutKey = step.getOutputOperands().get("totalAmount");
        if (taxAmountOutKey == null || totalAmountOutKey == null) {
            throw new IllegalArgumentException("Output operand keys for 'taxAmount'/'totalAmount' are not configured in TAX schema step");
        }

        BigDecimal taxableAmount = (BigDecimal) operands.get(amountInKey);
        if (taxableAmount == null) {
            throw new NoSuchElementException("Taxable amount operand not found: " + amountInKey);
        }

        // Get tax rate from step configuration using BigDecimal precision (default to 8% / 0.08)
        BigDecimal taxRate = new BigDecimal("0.08");
        if (step.getStepConfig() != null && step.getStepConfig().containsKey("taxRate")) {
            Object rateObj = step.getStepConfig().get("taxRate");
            if (rateObj instanceof BigDecimal) {
                taxRate = (BigDecimal) rateObj;
            } else if (rateObj instanceof Number) {
                taxRate = new BigDecimal(rateObj.toString());
            } else if (rateObj instanceof String) {
                taxRate = new BigDecimal((String) rateObj);
            }
        }

        BigDecimal taxAmount = taxableAmount.multiply(taxRate).setScale(2, RoundingMode.HALF_UP);
        BigDecimal totalAmount = taxableAmount.add(taxAmount).setScale(2, RoundingMode.HALF_UP);

        operands.put(taxAmountOutKey, taxAmount);
        operands.put(totalAmountOutKey, totalAmount);
        operands.put("TAX_AMOUNT", taxAmount);
        operands.put("TOTAL_AMOUNT", totalAmount);
    }
}
