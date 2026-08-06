package com.evn.billing.common.util;

import com.evn.billing.common.dto.AnomalyResult;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;

public class SmartAnomalyDetector {

    private static final BigDecimal ALPHA = new BigDecimal("0.3");
    private static final BigDecimal Z_THRESHOLD = new BigDecimal("2.5");

    /**
     * Detects consumption anomalies using Exponential Moving Average (EMA) and Z-Score.
     * Fallback to static 30% deviation check if historical data size is less than 3.
     *
     * @param current The current period consumption
     * @param history The historical consumptions list (ordered from oldest to newest)
     * @return AnomalyResult containing detection details
     */
    public AnomalyResult detect(BigDecimal current, List<BigDecimal> history) {
        if (current == null) {
            current = BigDecimal.ZERO;
        }

        // Fallback case: if history is too short, perform simple 30% deviation check
        if (history == null || history.size() < 3) {
            BigDecimal avg = BigDecimal.ZERO;
            if (history != null && !history.isEmpty()) {
                BigDecimal sum = BigDecimal.ZERO;
                for (BigDecimal h : history) {
                    sum = sum.add(h);
                }
                avg = sum.divide(BigDecimal.valueOf(history.size()), 4, RoundingMode.HALF_UP);
            }

            boolean isAnomaly = false;
            if (avg.compareTo(new BigDecimal("50.00")) > 0) {
                BigDecimal diff = current.subtract(avg).abs();
                BigDecimal ratio = diff.divide(avg, 4, RoundingMode.HALF_UP);
                isAnomaly = ratio.compareTo(new BigDecimal("0.30")) > 0;
            }
            return new AnomalyResult(isAnomaly, BigDecimal.ZERO, avg, BigDecimal.ZERO);
        }

        // 1. Calculate EMA: EMA_t = alpha * Y_t + (1 - alpha) * EMA_{t-1}
        BigDecimal ema = history.get(0);
        BigDecimal oneMinusAlpha = BigDecimal.ONE.subtract(ALPHA);
        for (int i = 1; i < history.size(); i++) {
            BigDecimal val = history.get(i);
            ema = ALPHA.multiply(val).add(oneMinusAlpha.multiply(ema));
        }

        // 2. Calculate variance and standard deviation using EMA as center
        BigDecimal varianceSum = BigDecimal.ZERO;
        for (BigDecimal val : history) {
            BigDecimal diff = val.subtract(ema);
            varianceSum = varianceSum.add(diff.multiply(diff));
        }
        BigDecimal variance = varianceSum.divide(BigDecimal.valueOf(history.size()), 4, RoundingMode.HALF_UP);
        
        // Calculate square root of variance (Standard Deviation) using Newton-Raphson method
        BigDecimal stdDev = sqrt(variance);

        // 3. Calculate Z-Score: Z = (current - EMA) / stdDev
        BigDecimal zScore = BigDecimal.ZERO;
        boolean isAnomaly = false;

        if (stdDev.compareTo(new BigDecimal("0.01")) > 0) {
            zScore = current.subtract(ema).divide(stdDev, 4, RoundingMode.HALF_UP);
            isAnomaly = zScore.abs().compareTo(Z_THRESHOLD) > 0;
        } else {
            // Standard deviation is close to 0 (stable consumption), check absolute difference
            BigDecimal diff = current.subtract(ema).abs();
            if (ema.compareTo(BigDecimal.ZERO) > 0) {
                BigDecimal ratio = diff.divide(ema, 4, RoundingMode.HALF_UP);
                isAnomaly = ratio.compareTo(new BigDecimal("0.30")) > 0;
            } else {
                isAnomaly = diff.compareTo(new BigDecimal("10.00")) > 0; // arbitrary threshold if ema is 0
            }
        }

        return new AnomalyResult(isAnomaly, zScore, ema, stdDev);
    }

    private BigDecimal sqrt(BigDecimal value) {
        if (value.compareTo(BigDecimal.ZERO) < 0) {
            throw new IllegalArgumentException("Cannot calculate square root of a negative number");
        }
        if (value.compareTo(BigDecimal.ZERO) == 0) {
            return BigDecimal.ZERO;
        }
        BigDecimal num = value;
        BigDecimal two = BigDecimal.valueOf(2);
        // Initial guess
        BigDecimal x = value.divide(two, 4, RoundingMode.HALF_UP);
        for (int i = 0; i < 20; i++) {
            BigDecimal nextX = x.add(num.divide(x, 4, RoundingMode.HALF_UP)).divide(two, 4, RoundingMode.HALF_UP);
            if (nextX.subtract(x).abs().compareTo(new BigDecimal("0.0001")) < 0) {
                return nextX;
            }
            x = nextX;
        }
        return x;
    }
}
