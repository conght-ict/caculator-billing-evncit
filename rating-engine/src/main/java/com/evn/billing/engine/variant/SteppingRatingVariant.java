package com.evn.billing.engine.variant;

import com.evn.billing.common.dto.BillingSchemaStep;
import com.evn.billing.common.dto.TariffRules;
import com.evn.billing.engine.RatingStepEngine;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

public class SteppingRatingVariant implements BillingVariant {

    private final RatingStepEngine ratingStepEngine = new RatingStepEngine();

    @Override
    @SuppressWarnings("unchecked")
    public void execute(Map<String, Object> operands, BillingSchemaStep step) throws Exception {
        if (step.getInputOperands() == null) {
            throw new IllegalArgumentException("Input operands are not configured in schema step");
        }
        String consumptionKey = step.getInputOperands().get("consumption");
        String tariffCodeKey = step.getInputOperands().get("tariffCode");

        if (consumptionKey == null) {
            throw new IllegalArgumentException("Input operand key for 'consumption' is not configured in schema step");
        }
        if (tariffCodeKey == null) {
            throw new IllegalArgumentException("Input operand key for 'tariffCode' is not configured in schema step");
        }

        if (step.getOutputOperands() == null) {
            throw new IllegalArgumentException("Output operands are not configured in schema step");
        }
        String amountOutKey = step.getOutputOperands().get("amount");
        String breakdownOutKey = step.getOutputOperands().get("breakdown");
        if (amountOutKey == null) {
            throw new IllegalArgumentException("Output operand key for 'amount' is not configured in schema step");
        }
        if (breakdownOutKey == null) {
            throw new IllegalArgumentException("Output operand key for 'breakdown' is not configured in schema step");
        }

        BigDecimal consumption = (BigDecimal) operands.get(consumptionKey);
        if (consumption == null) {
            throw new IllegalArgumentException("Required operand '" + consumptionKey + "' (consumption) is missing in calculation context");
        }

        String tariffCode = (String) operands.get(tariffCodeKey);
        if (tariffCode == null) {
            throw new IllegalArgumentException("Required operand '" + tariffCodeKey + "' (tariffCode) is missing in calculation context");
        }

        if (operands.get("NORMS_FACTOR") == null) {
            throw new IllegalArgumentException("NORMS_FACTOR is required in operands but was not provided");
        }
        int normsFactor = (Integer) operands.get("NORMS_FACTOR");

        Map<String, TariffRules> tariffs = (Map<String, TariffRules>) operands.get("TARIFFS");
        if (tariffs == null) {
            throw new IllegalArgumentException("Required operand 'TARIFFS' is missing in calculation context");
        }

        TariffRules rules = tariffs.get(tariffCode);
        if (rules == null) {
            throw new IllegalArgumentException("Tariff configuration missing for code: " + tariffCode);
        }

        BigDecimal proRataFactor = (BigDecimal) operands.getOrDefault("PRO_RATA_FACTOR", BigDecimal.ONE);

        List<RatingStepEngine.StepResult> stepResults = ratingStepEngine.calculateSteppingTariff(
                consumption, rules.getBlocks(), normsFactor, proRataFactor);

        BigDecimal totalAmount = BigDecimal.ZERO;
        for (RatingStepEngine.StepResult r : stepResults) {
            totalAmount = totalAmount.add(r.getAmount());
        }
        totalAmount = totalAmount.setScale(2, RoundingMode.HALF_UP);

        // Store outputs in operands context
        operands.put(amountOutKey, totalAmount);
        operands.put(breakdownOutKey, stepResults);
    }
}
