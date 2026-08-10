package com.evn.billing.engine;

import com.evn.billing.common.dto.*;
import com.evn.billing.engine.variant.BillingVariant;
import com.evn.billing.engine.variant.VariantRegistry;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.time.LocalDate;
import java.time.YearMonth;
import java.time.temporal.ChronoUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BillingCalculator {

    private static final Logger log = LoggerFactory.getLogger(BillingCalculator.class);
    private final TopologyCalculator topologyCalculator = new TopologyCalculator();

    /**
     * Executes the billing rating calculations for a customer account using the SAP IS-U Billing Schema.
     */
    @SuppressWarnings("unchecked")
    public CalculationResult calculate(BillingConfigSnapshot config, Map<String, BigDecimal> consumptions) throws Exception {
        String month = "2026_06";
        if (config.getDenNgay() != null) {
            LocalDate toDate = config.getDenNgay();
            month = String.format("%d_%02d", toDate.getYear(), toDate.getMonthValue());
        }
        long days = 30;
        if (config.getTuNgay() != null && config.getDenNgay() != null) {
            days = ChronoUnit.DAYS.between(config.getTuNgay(), config.getDenNgay()) + 1;
            if (days <= 0) days = 30;
        }
        return calculate(config, consumptions, month, days);
    }

    /**
     * Overloaded method executing rating calculations with custom month and days used for pro-rata rules.
     */
    @SuppressWarnings("unchecked")
    public CalculationResult calculate(BillingConfigSnapshot config, Map<String, BigDecimal> consumptions, String billingCycleMonth, long daysUsed) throws Exception {
        Map<String, Object> meterPointBreakdowns = new HashMap<>();
        List<Map<String, Object>> stepDetails = new ArrayList<>();
        Map<String, BigDecimal> nodeNetConsumptions = new HashMap<>();

        if (config.getBieuGia() == null || config.getBieuGia().isEmpty()) {
            throw new IllegalStateException("Snapshot is missing tariff configuration for account: " + config.getMaKhang());
        }

        // Find the rating step configuration (typically step 10)
        BillingSchemaStep ratingStep = config.getSchemaSteps().stream()
                .filter(s -> "STEP_RATING".equals(s.getVariantName()) || "FLAT_RATING".equals(s.getVariantName()))
                .findFirst()
                .orElseThrow(() -> new NoSuchElementException("Billing Schema is missing a primary rating step (STEP_RATING / FLAT_RATING)"));

        // Compute pro-rata factor based on actual days in the billing month
        BigDecimal proRataFactor = BigDecimal.ONE;
        if (billingCycleMonth != null && billingCycleMonth.contains("_")) {
            try {
                String[] parts = billingCycleMonth.split("_");
                int year = Integer.parseInt(parts[0]);
                int monthVal = Integer.parseInt(parts[1]);
                YearMonth yearMonth = YearMonth.of(year, monthVal);
                int daysInMonth = yearMonth.lengthOfMonth();
                if (daysUsed < daysInMonth && daysUsed > 0) {
                    proRataFactor = BigDecimal.valueOf(daysUsed).divide(BigDecimal.valueOf(daysInMonth), 8, RoundingMode.HALF_UP);
                }
            } catch (Exception e) {
                // Ignore parsing errors and default to 1.0
            }
        }

        BigDecimal totalBaseAmount = BigDecimal.ZERO;

        // 🐢 UNIFIED PATH FOR ALL TOPOLOGIES (Single/Multiple Meters, Stepping, Flat, TOU, and Mixed Pricing)
        List<MeterPointNode> allNodes = new ArrayList<>();
        if (config.getMeterTopology() != null && config.getMeterTopology().getRootPoints() != null) {
            for (MeterPointNode root : config.getMeterTopology().getRootPoints()) {
                collectAllNodes(root, allNodes);
            }
        }

        // 1. Calculate net consumption for each node (netting & child aggregation)
        for (MeterPointNode node : allNodes) {
            BigDecimal net = topologyCalculator.calculateNetConsumption(node, consumptions);
            nodeNetConsumptions.put(node.getMaDdo(), net != null ? net : BigDecimal.ZERO);
        }

        // 2. Process rating for each node based on its pricing rules
        for (MeterPointNode node : allNodes) {
            BigDecimal net = nodeNetConsumptions.get(node.getMaDdo());
            if (net == null) net = BigDecimal.ZERO;

            List<PriceApplicationRule> rules = node.getPriceRules();
            
            // FIX-DC-MAIN: Skip AGGREGATION-ONLY nodes (no priceRules and no maNgia)
            if ((rules == null || rules.isEmpty()) && node.getMaNgia() == null) {
                log.info("[RATING-SKIP] Node {} has no pricing rules (aggregation-only node), skipping rating. Net consumption = {} kWh.",
                        node.getMaDdo(), net);
                Map<String, Object> nodeBreakdown = new HashMap<>();
                nodeBreakdown.put("meter_point_id", node.getMaDdo());
                nodeBreakdown.put("net_consumption", net);
                nodeBreakdown.put("amount", BigDecimal.ZERO);
                nodeBreakdown.put("steps", Collections.emptyList());
                nodeBreakdown.put("customer_case", "AGGREGATION_ONLY");
                meterPointBreakdowns.put(node.getMaDdo(), nodeBreakdown);
                continue;
            }

            if (isAggregationOnlyNode(node, consumptions)) {
                log.info("[RATING-SKIP] Node {} is aggregation-only parent (net={} kWh from child). Skipping rating.",
                        node.getMaDdo(), net);
                Map<String, Object> nodeBreakdown = new HashMap<>();
                nodeBreakdown.put("meter_point_id", node.getMaDdo());
                nodeBreakdown.put("net_consumption", net);
                nodeBreakdown.put("amount", BigDecimal.ZERO);
                nodeBreakdown.put("steps", Collections.emptyList());
                nodeBreakdown.put("customer_case", "AGGREGATION_ONLY");
                meterPointBreakdowns.put(node.getMaDdo(), nodeBreakdown);
                continue;
            }

            if (rules == null || rules.isEmpty()) {
                if (node.getMaNgia() == null) {
                    throw new IllegalArgumentException("No pricing rules or tariff code mapped for meter point: " + node.getMaDdo());
                }
                PriceApplicationRule defaultRule = new PriceApplicationRule();
                defaultRule.setMaNgia(node.getMaNgia());
                int normsFactor = config.getSoHo();
                if ("SINH_HOAT".equals(config.getCustomerType()) && normsFactor <= 0) {
                    throw new IllegalArgumentException("Norms factor must be a positive integer, but was " + normsFactor);
                }
                defaultRule.setSoHo(normsFactor);
                defaultRule.setTgianBdien("BT");
                rules = Collections.singletonList(defaultRule);
            }

            // FIX-BCS-01: Deduplicate rules to avoid duplicate calculations of same tariff on full consumption
            rules = deduplicateBcsRules(rules);

            String customerCase = classifyCustomerCase(node, rules, proRataFactor, config, net);
            log.info("MeterPointId: {} classified as customer case: {}", node.getMaDdo(), customerCase);

            BigDecimal remainingKwh = net;
            BigDecimal nodeAmount = BigDecimal.ZERO;
            List<RatingStepEngine.StepResult> nodeSteps = new ArrayList<>();

            for (PriceApplicationRule rule : rules) {
                BigDecimal allocatedKwh = BigDecimal.ZERO;
                
                // Fetch the consumption based on Time period or fall back to total node consumption
                BigDecimal sourceCons = net;
                if (rule.getTgianBdien() != null) {
                    String registerKey = node.getMaDdo() + "_" + rule.getTgianBdien();
                    if (consumptions.containsKey(registerKey)) {
                        sourceCons = consumptions.get(registerKey);
                    }
                }

                // Apply Mixed Pricing splits: TL (%) or SL (kWh volume) or full remainder
                if (("TL".equals(rule.getLoaiDmuc()) || "%".equals(rule.getLoaiDmuc())) && rule.getDinhMuc() != null) {
                    allocatedKwh = sourceCons.multiply(rule.getDinhMuc())
                            .divide(BigDecimal.valueOf(100), 2, RoundingMode.HALF_UP);
                } else if (("SL".equals(rule.getLoaiDmuc()) || "Kwh".equalsIgnoreCase(rule.getLoaiDmuc()) || "C".equals(rule.getLoaiDmuc())) && rule.getDinhMuc() != null) {
                    BigDecimal proRataLimit = rule.getDinhMuc().multiply(proRataFactor);
                    allocatedKwh = remainingKwh.min(proRataLimit);
                    remainingKwh = remainingKwh.subtract(allocatedKwh);
                } else {
                    allocatedKwh = remainingKwh;
                    remainingKwh = BigDecimal.ZERO;
                }

                String tariffCode = rule.getMaNgia();
                TariffRules tariffRules = config.getBieuGia().get(tariffCode);
                if (tariffRules == null) {
                    throw new IllegalStateException("Missing tariff rules configuration for code: " + tariffCode
                            + " (account: " + config.getMaKhang() + ", meter: " + node.getMaDdo() + ")");
                }

                List<RatingStepEngine.StepResult> ruleSteps;
                Map<String, Object> operands = new HashMap<>();
                operands.put("NET_KWH", allocatedKwh);
                operands.put("FAST_TARIFF_CODE", tariffCode);
                operands.put("NORMS_FACTOR", rule.getSoHo());
                operands.put("PRO_RATA_FACTOR", proRataFactor);
                operands.put("TARIFFS", config.getBieuGia());

                if (tariffRules.isBacThang()) {
                    BillingVariant variant = VariantRegistry.get("STEP_RATING");
                    variant.execute(operands, ratingStep);
                    ruleSteps = (List<RatingStepEngine.StepResult>) operands.get(ratingStep.getOutputOperands().get("breakdown"));
                } else {
                    BillingVariant variant = VariantRegistry.get("FLAT_RATING");
                    variant.execute(operands, ratingStep);
                    ruleSteps = (List<RatingStepEngine.StepResult>) operands.get(ratingStep.getOutputOperands().get("breakdown"));
                }

                for (RatingStepEngine.StepResult r : ruleSteps) {
                    nodeAmount = nodeAmount.add(r.getAmount());
                    nodeSteps.add(r);

                    Map<String, Object> sd = new HashMap<>();
                    sd.put("meter_point_id", node.getMaDdo());
                    sd.put("step", r.getStep());
                    sd.put("kwh", r.getKwhConsumed());
                    sd.put("price", r.getUnitPrice());
                    sd.put("amount", r.getAmount());
                    sd.put("tariff_code", tariffCode);
                    sd.put("time_period", rule.getTgianBdien());
                    stepDetails.add(sd);
                }
            }

            totalBaseAmount = totalBaseAmount.add(nodeAmount);

            Map<String, Object> nodeBreakdown = new HashMap<>();
            nodeBreakdown.put("meter_point_id", node.getMaDdo());
            nodeBreakdown.put("net_consumption", net);
            nodeBreakdown.put("amount", nodeAmount);
            nodeBreakdown.put("steps", nodeSteps);
            nodeBreakdown.put("customer_case", customerCase);
            meterPointBreakdowns.put(node.getMaDdo(), nodeBreakdown);
        }

        // 3. Set up Account-Level Operands Context
        Map<String, Object> accountOperands = new HashMap<>();
        accountOperands.put("BASE_AMOUNT", totalBaseAmount);
        accountOperands.put("DISCOUNT_AMOUNT", BigDecimal.ZERO);
        accountOperands.put("TAX_AMOUNT", BigDecimal.ZERO);
        accountOperands.put("TOTAL_AMOUNT", totalBaseAmount);

        // 4. Execute remaining steps in the Billing Schema sequentially (e.g., Discount, Taxes)
        List<BillingSchemaStep> sortedSteps = new ArrayList<>(config.getSchemaSteps());
        sortedSteps.sort(Comparator.comparing(BillingSchemaStep::getStepNumber));

        for (BillingSchemaStep step : sortedSteps) {
            // Skip the rating step since it was already executed at the meter level
            if (step.getStepNumber() == ratingStep.getStepNumber()) {
                continue;
            }

            BillingVariant variant = VariantRegistry.get(step.getVariantName());
            variant.execute(accountOperands, step);
        }

        BigDecimal finalTotalBeforeTax = totalBaseAmount;
        BigDecimal finalTotalAfterTax = (BigDecimal) accountOperands.get("TOTAL_AMOUNT");
        BigDecimal finalTaxAmount = (BigDecimal) accountOperands.get("TAX_AMOUNT");
        BigDecimal finalDiscountAmount = (BigDecimal) accountOperands.get("DISCOUNT_AMOUNT");

        return new CalculationResult(
                finalTotalBeforeTax,
                finalTaxAmount,
                finalTotalAfterTax,
                finalDiscountAmount,
                meterPointBreakdowns,
                stepDetails,
                nodeNetConsumptions
        );
    }

    @SuppressWarnings("unchecked")
    public CalculationResult calculateFastPath(BillingConfigSnapshot config, Map<String, BigDecimal> consumptions, String billingCycleMonth, long daysUsed) throws Exception {
        Map<String, Object> meterPointBreakdowns = new HashMap<>();
        List<Map<String, Object>> stepDetails = new ArrayList<>();
        Map<String, BigDecimal> nodeNetConsumptions = new HashMap<>();

        if (config.getBieuGia() == null || config.getBieuGia().isEmpty()) {
            throw new IllegalStateException("Snapshot is missing tariff configuration for account: " + config.getMaKhang());
        }

        // Find the rating step configuration (typically step 10)
        BillingSchemaStep ratingStep = config.getSchemaSteps().stream()
                .filter(s -> "STEP_RATING".equals(s.getVariantName()) || "FLAT_RATING".equals(s.getVariantName()))
                .findFirst()
                .orElseThrow(() -> new NoSuchElementException("Billing Schema is missing a primary rating step (STEP_RATING / FLAT_RATING)"));

        // Compute pro-rata factor based on actual days in the billing month
        BigDecimal proRataFactor = BigDecimal.ONE;
        if (billingCycleMonth != null && billingCycleMonth.contains("_")) {
            try {
                String[] parts = billingCycleMonth.split("_");
                int year = Integer.parseInt(parts[0]);
                int monthVal = Integer.parseInt(parts[1]);
                YearMonth yearMonth = YearMonth.of(year, monthVal);
                int daysInMonth = yearMonth.lengthOfMonth();
                if (daysUsed < daysInMonth && daysUsed > 0) {
                    proRataFactor = BigDecimal.valueOf(daysUsed).divide(BigDecimal.valueOf(daysInMonth), 8, RoundingMode.HALF_UP);
                }
            } catch (Exception e) {
                // Ignore parsing errors and default to 1.0
            }
        }

        // Fast Path: assume single meter point node, no netting tree traversal
        if (config.getMeterTopology() == null 
                || config.getMeterTopology().getRootPoints() == null 
                || config.getMeterTopology().getRootPoints().isEmpty()) {
            throw new IllegalStateException("Fast-path requires at least one root meter point in topology");
        }

        MeterPointNode node = config.getMeterTopology().getRootPoints().get(0);
        BigDecimal net = consumptions.get(node.getMaDdo());
        if (net == null) {
            throw new IllegalStateException("No consumption found for fast-path meterPointId: " + node.getMaDdo());
        }
        nodeNetConsumptions.put(node.getMaDdo(), net);

        List<PriceApplicationRule> rules = node.getPriceRules();
        if (rules == null || rules.isEmpty()) {
            if (node.getMaNgia() == null) {
                throw new IllegalArgumentException("No pricing rules or tariff code mapped for fast path meter point: " + node.getMaDdo());
            }
            PriceApplicationRule defaultRule = new PriceApplicationRule();
            defaultRule.setMaNgia(node.getMaNgia());
            int normsFactor = config.getSoHo();
            if ("SINH_HOAT".equals(config.getCustomerType()) && normsFactor <= 0) {
                throw new IllegalArgumentException("Norms factor must be a positive integer, but was " + normsFactor);
            }
            defaultRule.setSoHo(normsFactor);
            defaultRule.setTgianBdien("BT");
            rules = Collections.singletonList(defaultRule);
        }

        // FIX-BCS-01: Deduplicate rules to avoid duplicate calculations of same tariff on full consumption
        rules = deduplicateBcsRules(rules);

        String customerCase = classifyCustomerCase(node, rules, proRataFactor, config, net);
        log.info("[FAST-PATH] MeterPointId: {} classified as customer case: {}", node.getMaDdo(), customerCase);

        BigDecimal remainingKwh = net;
        BigDecimal nodeAmount = BigDecimal.ZERO;
        List<RatingStepEngine.StepResult> nodeSteps = new ArrayList<>();

        for (PriceApplicationRule rule : rules) {
            BigDecimal allocatedKwh = BigDecimal.ZERO;
            BigDecimal sourceCons = net;
            if (rule.getTgianBdien() != null) {
                String registerKey = node.getMaDdo() + "_" + rule.getTgianBdien();
                if (consumptions.containsKey(registerKey)) {
                    sourceCons = consumptions.get(registerKey);
                }
            }

            if (("TL".equals(rule.getLoaiDmuc()) || "%".equals(rule.getLoaiDmuc())) && rule.getDinhMuc() != null) {
                allocatedKwh = sourceCons.multiply(rule.getDinhMuc())
                        .divide(BigDecimal.valueOf(100), 2, RoundingMode.HALF_UP);
            } else if (("SL".equals(rule.getLoaiDmuc()) || "Kwh".equalsIgnoreCase(rule.getLoaiDmuc()) || "C".equals(rule.getLoaiDmuc())) && rule.getDinhMuc() != null) {
                BigDecimal proRataLimit = rule.getDinhMuc().multiply(proRataFactor);
                allocatedKwh = remainingKwh.min(proRataLimit);
                remainingKwh = remainingKwh.subtract(allocatedKwh);
            } else {
                allocatedKwh = remainingKwh;
                remainingKwh = BigDecimal.ZERO;
            }

            String tariffCode = rule.getMaNgia();
            TariffRules tariffRules = config.getBieuGia().get(tariffCode);
            if (tariffRules == null) {
                throw new IllegalStateException("Missing tariff rules configuration for code: " + tariffCode
                        + " (account: " + config.getMaKhang() + ", meter: " + node.getMaDdo() + ")");
            }

            List<RatingStepEngine.StepResult> ruleSteps;
            Map<String, Object> operands = new HashMap<>();
            operands.put("NET_KWH", allocatedKwh);
            operands.put("FAST_TARIFF_CODE", tariffCode);
            operands.put("NORMS_FACTOR", rule.getSoHo());
            operands.put("PRO_RATA_FACTOR", proRataFactor);
            operands.put("TARIFFS", config.getBieuGia());

            if ("STEPPING".equals(tariffRules.getLoaiBieuGia())) {
                BillingVariant variant = VariantRegistry.get("STEP_RATING");
                variant.execute(operands, ratingStep);
                ruleSteps = (List<RatingStepEngine.StepResult>) operands.get(ratingStep.getOutputOperands().get("breakdown"));
            } else {
                BillingVariant variant = VariantRegistry.get("FLAT_RATING");
                variant.execute(operands, ratingStep);
                ruleSteps = (List<RatingStepEngine.StepResult>) operands.get(ratingStep.getOutputOperands().get("breakdown"));
            }

            for (RatingStepEngine.StepResult r : ruleSteps) {
                nodeAmount = nodeAmount.add(r.getAmount());
                nodeSteps.add(r);

                Map<String, Object> sd = new HashMap<>();
                sd.put("meter_point_id", node.getMaDdo());
                sd.put("step", r.getStep());
                sd.put("kwh", r.getKwhConsumed());
                sd.put("price", r.getUnitPrice());
                sd.put("amount", r.getAmount());
                sd.put("tariff_code", tariffCode);
                sd.put("time_period", rule.getTgianBdien());
                stepDetails.add(sd);
            }
        }

        BigDecimal totalBaseAmount = nodeAmount;

        Map<String, Object> nodeBreakdown = new HashMap<>();
        nodeBreakdown.put("meter_point_id", node.getMaDdo());
        nodeBreakdown.put("net_consumption", net);
        nodeBreakdown.put("amount", nodeAmount);
        nodeBreakdown.put("steps", nodeSteps);
        nodeBreakdown.put("customer_case", customerCase);
        meterPointBreakdowns.put(node.getMaDdo(), nodeBreakdown);

        // Account Level steps
        Map<String, Object> accountOperands = new HashMap<>();
        accountOperands.put("BASE_AMOUNT", totalBaseAmount);
        accountOperands.put("DISCOUNT_AMOUNT", BigDecimal.ZERO);
        accountOperands.put("TAX_AMOUNT", BigDecimal.ZERO);
        accountOperands.put("TOTAL_AMOUNT", totalBaseAmount);

        List<BillingSchemaStep> sortedSteps = new ArrayList<>(config.getSchemaSteps());
        sortedSteps.sort(Comparator.comparing(BillingSchemaStep::getStepNumber));

        for (BillingSchemaStep step : sortedSteps) {
            if (step.getStepNumber() == ratingStep.getStepNumber()) {
                continue;
            }
            BillingVariant variant = VariantRegistry.get(step.getVariantName());
            variant.execute(accountOperands, step);
        }

        BigDecimal finalTotalBeforeTax = totalBaseAmount;
        BigDecimal finalTotalAfterTax = (BigDecimal) accountOperands.get("TOTAL_AMOUNT");
        BigDecimal finalTaxAmount = (BigDecimal) accountOperands.get("TAX_AMOUNT");
        BigDecimal finalDiscountAmount = (BigDecimal) accountOperands.get("DISCOUNT_AMOUNT");

        return new CalculationResult(
                finalTotalBeforeTax,
                finalTaxAmount,
                finalTotalAfterTax,
                finalDiscountAmount,
                meterPointBreakdowns,
                stepDetails,
                nodeNetConsumptions
        );
    }

    private void collectAllNodes(MeterPointNode node, List<MeterPointNode> allNodes) {
        if (node == null) return;
        allNodes.add(node);
        if (node.getChildPoints() != null) {
            for (MeterPointNode child : node.getChildPoints()) {
                collectAllNodes(child, allNodes);
            }
        }
    }

    private String classifyCustomerCase(MeterPointNode node, List<PriceApplicationRule> rules, BigDecimal proRataFactor, BillingConfigSnapshot config, BigDecimal net) {
        String baseType = config.getCustomerType();
        if (baseType == null) {
            throw new IllegalArgumentException("Customer type (customerCategory) is missing in snapshot configuration for account: " + config.getMaKhang());
        }

        double X = net != null ? net.doubleValue() : 0.0;

        if ("SINH_HOAT".equals(baseType)) {
            int soHo = config.getSoHo();
            if (soHo <= 0) {
                throw new IllegalArgumentException("Norms factor must be a positive integer, but was " + soHo + " for account: " + config.getMaKhang());
            }
            // Determine reading fluctuations/variations
            if (proRataFactor != null && proRataFactor.compareTo(BigDecimal.ONE) < 0) {
                return "SINH_HOAT_THIEU_DINH_MUC"; // Thiếu định mức do lẻ ngày
            } else if (X < 50.0 * soHo) {
                return "SINH_HOAT_THIEU_DINH_MUC"; // Thiếu định mức do sản lượng thấp dưới bậc 1
            } else {
                return "SINH_HOAT_DU_DINH_MUC"; // Dùng đủ định mức chuẩn
            }
        } else if ("NGOAI_SINH_HOAT".equals(baseType)) {
            return "NGOAI_SINH_HOAT_100";
        } else {
            // Mixed pricing cases (MIXED)
            String normType = null;
            for (PriceApplicationRule rule : rules) {
                if (rule.getLoaiDmuc() != null) {
                    normType = rule.getLoaiDmuc();
                }
            }
            if ("TL".equals(normType) || "%".equals(normType)) {
                return "NGOAI_SINH_HOAT_MIX_SHBT_TL";
            } else if ("SL".equals(normType) || "Kwh".equalsIgnoreCase(normType) || "C".equals(normType)) {
                return "NGOAI_SINH_HOAT_MIX_SHBT_SL";
            } else {
                return "NGOAI_SINH_HOAT_MIX_SHBT";
            }
        }
    }

    private boolean isAggregationOnlyNode(MeterPointNode node, Map<String, BigDecimal> consumptions) {
        if (node.getChildPoints() == null || node.getChildPoints().isEmpty()) {
            return false;
        }
        boolean hasNettingChild = node.getChildPoints().stream()
            .anyMatch(c -> c.getCalculationType() == CalculationType.NETTING);
        if (hasNettingChild) {
            return false;
        }
        BigDecimal nodeRaw = consumptions.getOrDefault(node.getMaDdo(), BigDecimal.ZERO);
        return nodeRaw.compareTo(BigDecimal.ZERO) <= 0;
    }

    private List<PriceApplicationRule> deduplicateBcsRules(List<PriceApplicationRule> rules) {
        if (rules == null || rules.isEmpty()) {
            return rules;
        }
        Map<String, PriceApplicationRule> deduped = new LinkedHashMap<>();
        for (PriceApplicationRule r : rules) {
            String key = r.getMaNgia() + "|" + r.getTgianBdien() + "|" + r.getLoaiDmuc();
            deduped.putIfAbsent(key, r);
        }
        return new ArrayList<>(deduped.values());
    }
}
