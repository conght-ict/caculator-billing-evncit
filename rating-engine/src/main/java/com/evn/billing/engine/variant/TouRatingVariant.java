package com.evn.billing.engine.variant;

import com.evn.billing.common.dto.BillingSchemaStep;
import com.evn.billing.common.dto.TariffBlock;
import com.evn.billing.common.dto.TariffRules;
import com.evn.billing.engine.RatingStepEngine.StepResult;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

public class TouRatingVariant implements BillingVariant {

    @Override
    @SuppressWarnings("unchecked")
    public void execute(Map<String, Object> operands, BillingSchemaStep step) throws Exception {
        String consumptionKey = step.getInputOperands().get("consumption"); // e.g., "TOU_CONSUMPTIONS"
        String tariffCodeKey = step.getInputOperands().get("tariffCode");

        String amountOutKey = step.getOutputOperands().get("amount");
        String breakdownOutKey = step.getOutputOperands().get("breakdown");

        Map<String, BigDecimal> touConsumptions = (Map<String, BigDecimal>) operands.get(consumptionKey);
        String tariffCode = (String) operands.get(tariffCodeKey);
        Map<String, TariffRules> tariffs = (Map<String, TariffRules>) operands.get("TARIFFS");

        if (touConsumptions == null || tariffCode == null || tariffs == null) {
            throw new NoSuchElementException("Missing parameters for TouRatingVariant execution");
        }

        TariffRules rules = tariffs.get(tariffCode);
        if (rules == null) {
            throw new NoSuchElementException("Tariff configuration missing for code: " + tariffCode);
        }

        List<StepResult> stepResults = new ArrayList<>();
        BigDecimal totalAmount = BigDecimal.ZERO;

        for (TariffBlock block : rules.getBlocks()) {
            String touPeriod = block.getTgianBdien(); // PEAK, OFF_PEAK, NORMAL
            String consumptionMapKey = mapTouPeriodToKey(touPeriod);

            BigDecimal kwh = touConsumptions.getOrDefault(consumptionMapKey, BigDecimal.ZERO);
            if (kwh.compareTo(BigDecimal.ZERO) > 0) {
                StepResult result = new StepResult();
                result.setStep(block.getSoThuTu());
                result.setKwhConsumed(kwh.setScale(2, RoundingMode.HALF_UP));
                result.setUnitPrice(block.getDonGia());
                result.setAmount(result.getKwhConsumed().multiply(result.getUnitPrice()).setScale(2, RoundingMode.HALF_UP));
                
                stepResults.add(result);
                totalAmount = totalAmount.add(result.getAmount());
            }
        }

        totalAmount = totalAmount.setScale(2, RoundingMode.HALF_UP);

        operands.put(amountOutKey, totalAmount);
        operands.put(breakdownOutKey, stepResults);
    }

    private String mapTouPeriodToKey(String touPeriod) {
        if (touPeriod == null) return "BT";
        switch (touPeriod.toUpperCase()) {
            case "PEAK":
                return "CD";
            case "OFF_PEAK":
                return "TD";
            case "NORMAL":
            default:
                return "BT";
        }
    }
}
