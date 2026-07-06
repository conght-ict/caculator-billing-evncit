package com.evn.billing.engine;

import com.evn.billing.common.dto.TariffBlock;
import lombok.Data;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;

public class RatingStepEngine {

    @Data
    public static class StepResult {
        private int step;
        private BigDecimal kwhConsumed;
        private BigDecimal unitPrice;
        private BigDecimal amount;
    }

    /**
     * Calculates the charges for stepping tariff blocks, scaled by the norms factor.
     * 
     * @param netConsumption The total net consumption in kWh
     * @param standardBlocks The standard price blocks defined in the tariff configuration
     * @param normsFactor The number of sharing households (must be >= 1)
     * @return A list of StepResult containing consumption and charges per tier
     */
    public List<StepResult> calculateSteppingTariff(BigDecimal netConsumption, List<TariffBlock> standardBlocks, int normsFactor) {
        return calculateSteppingTariff(netConsumption, standardBlocks, normsFactor, BigDecimal.ONE);
    }

    /**
     * Calculates the charges for stepping tariff blocks, scaled by the norms factor and pro-rata factor.
     */
    public List<StepResult> calculateSteppingTariff(BigDecimal netConsumption, List<TariffBlock> standardBlocks, int normsFactor, BigDecimal proRataFactor) {
        List<StepResult> results = new ArrayList<>();
        BigDecimal remainingKwh = netConsumption;
        
        // Safeguard: normsFactor must be at least 1
        int effectiveNorms = Math.max(1, normsFactor);
        BigDecimal norms = BigDecimal.valueOf(effectiveNorms);
        BigDecimal scale = norms.multiply(proRataFactor != null ? proRataFactor : BigDecimal.ONE);

        for (TariffBlock block : standardBlocks) {
            if (remainingKwh.compareTo(BigDecimal.ZERO) <= 0) {
                break;
            }

            // Scale tier limits by norms factor and pro-rata factor
            BigDecimal blockMin = BigDecimal.valueOf(block.getMinKwh()).multiply(scale);
            BigDecimal blockMax = block.getMaxKwh() == null ? null : BigDecimal.valueOf(block.getMaxKwh()).multiply(scale);
            BigDecimal blockWidth = blockMax == null ? null : blockMax.subtract(blockMin);

            BigDecimal kwhInThisStep;
            if (blockWidth == null || remainingKwh.compareTo(blockWidth) < 0) {
                kwhInThisStep = remainingKwh;
            } else {
                kwhInThisStep = blockWidth;
            }

            StepResult result = new StepResult();
            result.setStep(block.getStep());
            result.setKwhConsumed(kwhInThisStep.setScale(2, RoundingMode.HALF_UP));
            result.setUnitPrice(BigDecimal.valueOf(block.getUnitPrice()));
            result.setAmount(kwhInThisStep.multiply(result.getUnitPrice()).setScale(2, RoundingMode.HALF_UP));
            
            results.add(result);
            remainingKwh = remainingKwh.subtract(kwhInThisStep);
        }

        return results;
    }
}
