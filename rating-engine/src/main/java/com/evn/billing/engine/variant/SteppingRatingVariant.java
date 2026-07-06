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
        String consumptionKey = step.getInputOperands().get("consumption");
        String tariffCodeKey = step.getInputOperands().get("tariffCode");

        String amountOutKey = step.getOutputOperands().get("amount");
        String breakdownOutKey = step.getOutputOperands().get("breakdown");

        BigDecimal consumption = (BigDecimal) operands.get(consumptionKey);
        String tariffCode = (String) operands.get(tariffCodeKey);
        int normsFactor = (Integer) operands.getOrDefault("NORMS_FACTOR", 1);
        Map<String, TariffRules> tariffs = (Map<String, TariffRules>) operands.get("TARIFFS");

        if (consumption == null || tariffCode == null || tariffs == null) {
            throw new NoSuchElementException("Missing parameters for SteppingRatingVariant execution");
        }

        TariffRules rules = tariffs.get(tariffCode);
        if (rules == null) {
            throw new NoSuchElementException("Tariff configuration missing for code: " + tariffCode);
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
