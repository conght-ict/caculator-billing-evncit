package com.evn.billing.engine;

import com.evn.billing.common.dto.BillingConfigSnapshot;
import com.evn.billing.common.dto.MeterPointNode;
import com.evn.billing.common.dto.MeterTopology;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ProrationEngine {

    private final BillingCalculator calculator = new BillingCalculator();

    /**
     * Calculates pro-rata billing when a price change occurs in the middle of the
     * billing cycle.
     */
    public CalculationResult calculateProrated(
            BillingConfigSnapshot config,
            Map<String, BigDecimal> consumptions,
            LocalDate startDate,
            LocalDate endDate,
            LocalDate changeDate,
            String tariffCodeBefore,
            String tariffCodeAfter,
            String cycleMonth) throws Exception {

        // 1. Calculate total days in cycle and days in each sub-period
        long totalDays = ChronoUnit_DAYS_between_plus_one(startDate, endDate);
        if (totalDays <= 0) {
            throw new IllegalArgumentException("Invalid date range: start date must be before or equal to end date.");
        }

        // Period 1: [startDate, changeDate - 1] (Price Before)
        long days1 = ChronoUnit_DAYS_between_plus_one(startDate, changeDate.minusDays(1));
        // Period 2: [changeDate, endDate] (Price After)
        long days2 = ChronoUnit_DAYS_between_plus_one(changeDate, endDate);

        if (days1 <= 0 || days2 <= 0) {
            // No split needed, either all days are before or after the change date
            return calculator.calculate(config, consumptions, cycleMonth, totalDays);
        }

        BigDecimal totalDaysBd = BigDecimal.valueOf(totalDays);
        BigDecimal factor1 = BigDecimal.valueOf(days1).divide(totalDaysBd, 8, RoundingMode.HALF_UP);
        BigDecimal factor2 = BigDecimal.valueOf(days2).divide(totalDaysBd, 8, RoundingMode.HALF_UP);

        // 2. Allocate consumptions proportionately
        Map<String, BigDecimal> consumptions1 = new HashMap<>();
        Map<String, BigDecimal> consumptions2 = new HashMap<>();
        for (Map.Entry<String, BigDecimal> entry : consumptions.entrySet()) {
            BigDecimal val = entry.getValue();
            consumptions1.put(entry.getKey(), val.multiply(factor1).setScale(4, RoundingMode.HALF_UP));
            consumptions2.put(entry.getKey(), val.multiply(factor2).setScale(4, RoundingMode.HALF_UP));
        }

        // 3. Compute for Period 1
        CalculationResult res1 = calculateSubPeriod(config, consumptions1, days1, tariffCodeBefore, cycleMonth);

        // 4. Compute for Period 2
        CalculationResult res2 = calculateSubPeriod(config, consumptions2, days2, tariffCodeAfter, cycleMonth);

        // 5. Merge results
        CalculationResult merged = new CalculationResult();
        merged.setTotalAmountBeforeTax(res1.getTotalAmountBeforeTax().add(res2.getTotalAmountBeforeTax()));
        merged.setTaxAmount(res1.getTaxAmount().add(res2.getTaxAmount()));
        merged.setTotalAmountAfterTax(res1.getTotalAmountAfterTax().add(res2.getTotalAmountAfterTax()));
        merged.setDiscountAmount(res1.getDiscountAmount().add(res2.getDiscountAmount()));

        // Merge stepDetails with tag indicating period
        List<Map<String, Object>> stepDetails = new ArrayList<>();
        if (res1.getStepDetails() != null) {
            for (Map<String, Object> step : res1.getStepDetails()) {
                Map<String, Object> copy = new HashMap<>(step);
                copy.put("prorationPeriod", "BEFORE_CHANGE");
                stepDetails.add(copy);
            }
        }
        if (res2.getStepDetails() != null) {
            for (Map<String, Object> step : res2.getStepDetails()) {
                Map<String, Object> copy = new HashMap<>(step);
                copy.put("prorationPeriod", "AFTER_CHANGE");
                stepDetails.add(copy);
            }
        }
        merged.setStepDetails(stepDetails);

        // Merge nodeNetConsumptions
        Map<String, BigDecimal> mergedNet = new HashMap<>();
        if (res1.getNodeNetConsumptions() != null) {
            for (Map.Entry<String, BigDecimal> e : res1.getNodeNetConsumptions().entrySet()) {
                mergedNet.merge(e.getKey(), e.getValue(), BigDecimal::add);
            }
        }
        if (res2.getNodeNetConsumptions() != null) {
            for (Map.Entry<String, BigDecimal> e : res2.getNodeNetConsumptions().entrySet()) {
                mergedNet.merge(e.getKey(), e.getValue(), BigDecimal::add);
            }
        }
        merged.setNodeNetConsumptions(mergedNet);

        // Merge meterPointBreakdowns
        Map<String, Object> mergedBreakdowns = new HashMap<>();
        if (res1.getMeterPointBreakdowns() != null) {
            mergedBreakdowns.put("BEFORE_CHANGE", res1.getMeterPointBreakdowns());
        }
        if (res2.getMeterPointBreakdowns() != null) {
            mergedBreakdowns.put("AFTER_CHANGE", res2.getMeterPointBreakdowns());
        }
        merged.setMeterPointBreakdowns(mergedBreakdowns);

        return merged;
    }

    private CalculationResult calculateSubPeriod(
            BillingConfigSnapshot config,
            Map<String, BigDecimal> consumptions,
            long daysUsed,
            String tariffCodeOverride,
            String cycleMonth) throws Exception {

        // Deep clone BillingConfigSnapshot to isolate modifications
        BillingConfigSnapshot copy = copySnapshot(config);

        // Override tariffCode in cloned meter topology
        if (copy.getMeterTopology() != null && copy.getMeterTopology().getRootPoints() != null) {
            for (MeterPointNode root : copy.getMeterTopology().getRootPoints()) {
                overrideTariffCode(root, tariffCodeOverride);
            }
        }

        return calculator.calculate(copy, consumptions, cycleMonth, daysUsed);
    }

    private void overrideTariffCode(MeterPointNode node, String tariffCode) {
        if (node.getMaNgia() != null) {
            node.setMaNgia(tariffCode);
        }
        if (node.getChildPoints() != null) {
            for (MeterPointNode child : node.getChildPoints()) {
                overrideTariffCode(child, tariffCode);
            }
        }
    }

    private BillingConfigSnapshot copySnapshot(BillingConfigSnapshot src) {
        BillingConfigSnapshot dest = new BillingConfigSnapshot();
        dest.setMaKhang(src.getMaKhang());
        dest.setDtuongQly(src.getDtuongQly());
        dest.setSoHo(src.getSoHo());
        dest.setLoaiKhangStr(src.getLoaiKhangStr());
        dest.setNgayHieuLuc(src.getNgayHieuLuc());
        dest.setTuNgay(src.getTuNgay());
        dest.setDenNgay(src.getDenNgay());
        dest.setMeterTopology(cloneTopology(src.getMeterTopology()));
        dest.setBieuGia(src.getBieuGia());
        dest.setSchemaSteps(src.getSchemaSteps());
        dest.setFastPathEnabled(src.isFastPathEnabled());
        dest.setFastPathMaDdo(src.getFastPathMaDdo());
        dest.setFastPathMaNgia(src.getFastPathMaNgia());
        return dest;
    }

    private MeterTopology cloneTopology(MeterTopology src) {
        if (src == null)
            return null;
        MeterTopology dest = new MeterTopology();
        if (src.getRootPoints() != null) {
            List<MeterPointNode> roots = new ArrayList<>();
            for (MeterPointNode r : src.getRootPoints()) {
                roots.add(cloneNode(r));
            }
            dest.setRootPoints(roots);
        }
        return dest;
    }

    private MeterPointNode cloneNode(MeterPointNode src) {
        if (src == null)
            return null;
        MeterPointNode dest = new MeterPointNode();
        dest.setMaDdo(src.getMaDdo());
        dest.setCalculationType(src.getCalculationType());
        dest.setMaNgia(src.getMaNgia());
        dest.setPriceRules(src.getPriceRules());
        if (src.getChildPoints() != null) {
            List<MeterPointNode> children = new ArrayList<>();
            for (MeterPointNode c : src.getChildPoints()) {
                children.add(cloneNode(c));
            }
            dest.setChildPoints(children);
        }
        return dest;
    }

    private long ChronoUnit_DAYS_between_plus_one(LocalDate start, LocalDate end) {
        if (start.isAfter(end))
            return 0;
        return ChronoUnit.DAYS.between(start, end) + 1;
    }
}
