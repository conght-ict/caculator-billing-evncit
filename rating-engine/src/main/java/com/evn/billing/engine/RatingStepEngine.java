package com.evn.billing.engine;

import com.evn.billing.common.dto.TariffBlock;
import lombok.Data;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class RatingStepEngine {

    @Data
    public static class StepResult {
        private int step;
        private BigDecimal kwhConsumed;
        private BigDecimal unitPrice;
        private BigDecimal amount;
    }

    private static final ConcurrentHashMap<String, PreparedTariff> cache = new ConcurrentHashMap<>();

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
        if (netConsumption == null || netConsumption.compareTo(BigDecimal.ZERO) <= 0 || standardBlocks == null || standardBlocks.isEmpty()) {
            return new ArrayList<>();
        }
        String cacheKey = buildCacheKey(standardBlocks);
        PreparedTariff prepared = cache.computeIfAbsent(cacheKey, k -> new PreparedTariff(standardBlocks));
        return prepared.calculate(netConsumption, normsFactor, proRataFactor);
    }

    private static String buildCacheKey(List<TariffBlock> blocks) {
        return blocks.stream()
                .map(b -> (b.getMinKwh() != null ? b.getMinKwh().toPlainString() : "0") + "|" +
                          (b.getMaxKwh() != null ? b.getMaxKwh().toPlainString() : "INF") + "|" +
                          (b.getDonGia() != null ? b.getDonGia().toPlainString() : "0"))
                .collect(Collectors.joining(";"));
    }

    private static class PreparedTariff {
        private final BigDecimal[] thresholds; // L
        private final BigDecimal[] widths;     // W
        private final BigDecimal[] prices;     // U
        private final int totalSteps;

        public PreparedTariff(List<TariffBlock> blocks) {
            this.totalSteps = blocks.size();
            this.thresholds = new BigDecimal[totalSteps - 1];
            this.widths = new BigDecimal[totalSteps - 1];
            this.prices = new BigDecimal[totalSteps];

            for (int i = 0; i < totalSteps; i++) {
                this.prices[i] = blocks.get(i).getDonGia();
            }

            BigDecimal accumWidth = BigDecimal.ZERO;
            for (int i = 0; i < totalSteps - 1; i++) {
                TariffBlock block = blocks.get(i);
                BigDecimal maxKwh = block.getMaxKwh() != null ? block.getMaxKwh() : BigDecimal.ZERO;
                BigDecimal minKwh = block.getMinKwh() != null ? block.getMinKwh() : BigDecimal.ZERO;
                BigDecimal width = maxKwh.subtract(minKwh);

                this.widths[i] = width;
                accumWidth = accumWidth.add(width);
                this.thresholds[i] = accumWidth;
            }
        }

        public List<StepResult> calculate(BigDecimal netConsumption, int normsFactor, BigDecimal proRataFactor) {
            int effectiveNorms = Math.max(1, normsFactor);
            BigDecimal H = BigDecimal.valueOf(effectiveNorms).multiply(proRataFactor != null ? proRataFactor : BigDecimal.ONE);
            BigDecimal X = netConsumption;
            
            // Tránh chia cho 0
            BigDecimal xNorm = H.compareTo(BigDecimal.ZERO) == 0 ? BigDecimal.ZERO : X.divide(H, 8, RoundingMode.HALF_UP);

            // Tìm bậc hiện tại (O(1))
            int stepIdx = -1;
            for (int i = thresholds.length - 1; i >= 0; i--) {
                if (xNorm.compareTo(thresholds[i]) > 0) {
                    stepIdx = i;
                    break;
                }
            }

            List<StepResult> results = new ArrayList<>(totalSteps);
            for (int j = 0; j < totalSteps; j++) {
                BigDecimal kwh;
                if (j <= stepIdx) {
                    // Bậc đã dùng hết
                    kwh = widths[j].multiply(H);
                } else if (j == stepIdx + 1) {
                    // Bậc đang dùng dở dang
                    BigDecimal previousLimit = (stepIdx == -1) ? BigDecimal.ZERO : thresholds[stepIdx];
                    kwh = X.subtract(previousLimit.multiply(H));
                } else {
                    // Bậc chưa dùng đến
                    kwh = BigDecimal.ZERO;
                }

                kwh = kwh.setScale(4, RoundingMode.HALF_UP);

                if (kwh.compareTo(new BigDecimal("0.0001")) > 0) {
                    StepResult result = new StepResult();
                    result.setStep(j + 1);
                    result.setKwhConsumed(kwh.setScale(2, RoundingMode.HALF_UP));
                    result.setUnitPrice(prices[j]);
                    result.setAmount(result.getKwhConsumed().multiply(result.getUnitPrice()).setScale(2, RoundingMode.HALF_UP));
                    results.add(result);
                }
            }
            return results;
        }
    }
}
