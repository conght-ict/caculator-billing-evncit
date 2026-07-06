package com.evn.billing.engine;

import com.evn.billing.common.dto.BillingConfigSnapshot;
import com.evn.billing.common.dto.BillingSchemaStep;
import com.evn.billing.common.dto.MeterPointNode;
import com.evn.billing.common.dto.TariffRules;
import com.evn.billing.engine.variant.BillingVariant;
import com.evn.billing.engine.variant.VariantRegistry;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;

public class BillingCalculator {

    private final TopologyCalculator topologyCalculator = new TopologyCalculator();

    /**
     * Executes the billing rating calculations for a customer account using the SAP IS-U Billing Schema.
     */
    @SuppressWarnings("unchecked")
    public CalculationResult calculate(BillingConfigSnapshot config, Map<String, BigDecimal> consumptions) throws Exception {
        return calculate(config, consumptions, "2026_06", 30);
    }

    /**
     * Overloaded method executing rating calculations with custom month and days used for pro-rata rules.
     */
    @SuppressWarnings("unchecked")
    public CalculationResult calculate(BillingConfigSnapshot config, Map<String, BigDecimal> consumptions, String billingCycleMonth, long daysUsed) throws Exception {
        Map<String, Object> meterPointBreakdowns = new HashMap<>();
        List<Map<String, Object>> stepDetails = new ArrayList<>();
        Map<String, BigDecimal> nodeNetConsumptions = new HashMap<>();

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
                java.time.YearMonth yearMonth = java.time.YearMonth.of(year, monthVal);
                int daysInMonth = yearMonth.lengthOfMonth();
                if (daysUsed < daysInMonth && daysUsed > 0) {
                    proRataFactor = BigDecimal.valueOf(daysUsed).divide(BigDecimal.valueOf(daysInMonth), 8, RoundingMode.HALF_UP);
                }
            } catch (Exception e) {
                // Ignore parsing errors and default to 1.0
            }
        }

        BigDecimal totalBaseAmount = BigDecimal.ZERO;

        if (config.isFastPathEnabled()) {
            // ⚡ FAST PATH (Only 1 meter, relations are empty)
            BigDecimal rawCons = consumptions.get(config.getFastPathMeterPointId());
            if (rawCons == null) {
                throw new NoSuchElementException("Reading missing for fast-path meter: " + config.getFastPathMeterPointId());
            }
            nodeNetConsumptions.put(config.getFastPathMeterPointId(), rawCons);

            // Execute the rating step variant
            Map<String, Object> operands = new HashMap<>();
            operands.put("NET_KWH", rawCons);
            operands.put("FAST_TARIFF_CODE", config.getFastPathTariffCode());
            operands.put("NORMS_FACTOR", config.getNormsFactor());
            operands.put("PRO_RATA_FACTOR", proRataFactor);
            operands.put("TARIFFS", config.getTariffs());

            BillingVariant variant = VariantRegistry.get(ratingStep.getVariantName());
            variant.execute(operands, ratingStep);

            BigDecimal amount = (BigDecimal) operands.get(ratingStep.getOutputOperands().get("amount"));
            List<RatingStepEngine.StepResult> steps = (List<RatingStepEngine.StepResult>) operands.get(ratingStep.getOutputOperands().get("breakdown"));

            totalBaseAmount = amount;

            Map<String, Object> nodeBreakdown = new HashMap<>();
            nodeBreakdown.put("meter_point_id", config.getFastPathMeterPointId());
            nodeBreakdown.put("tariff_code", config.getFastPathTariffCode());
            nodeBreakdown.put("net_consumption", rawCons);
            nodeBreakdown.put("amount", amount);
            nodeBreakdown.put("steps", steps);
            meterPointBreakdowns.put(config.getFastPathMeterPointId(), nodeBreakdown);

            for (RatingStepEngine.StepResult r : steps) {
                Map<String, Object> sd = new HashMap<>();
                sd.put("meter_point_id", config.getFastPathMeterPointId());
                sd.put("step", r.getStep());
                sd.put("kwh", r.getKwhConsumed());
                sd.put("price", r.getUnitPrice());
                sd.put("amount", r.getAmount());
                stepDetails.add(sd);
            }
        } else {
            // 🐢 SLOW PATH / MULTIPLE METERS
            // Calculate net consumption for all nodes recursively
            List<MeterPointNode> allNodes = new ArrayList<>();
            if (config.getMeterTopology() != null && config.getMeterTopology().getRootPoints() != null) {
                for (MeterPointNode root : config.getMeterTopology().getRootPoints()) {
                    collectAllNodes(root, allNodes);
                }
            }

            for (MeterPointNode node : allNodes) {
                BigDecimal net = topologyCalculator.calculateNetConsumption(node, consumptions);
                nodeNetConsumptions.put(node.getMeterPointId(), net != null ? net : BigDecimal.ZERO);
            }

            // Separate stepping nodes (which must be aggregated under STEPPING/SHBT rule) and flat nodes (TOU/manufacturing)
            List<MeterPointNode> steppingNodes = new ArrayList<>();
            List<MeterPointNode> flatNodes = new ArrayList<>();

            for (MeterPointNode node : allNodes) {
                if (node.getTariffCode() != null) {
                    TariffRules rules = config.getTariffs().get(node.getTariffCode());
                    if (rules != null && "STEPPING".equals(rules.getType())) {
                        steppingNodes.add(node);
                    } else {
                        flatNodes.add(node);
                    }
                }
            }

            // Execute aggregated stepping rating calculation exactly once for all stepping nodes sharing TARIFF_SHBT
            if (!steppingNodes.isEmpty()) {
                BigDecimal totalSteppingNet = BigDecimal.ZERO;
                for (MeterPointNode sn : steppingNodes) {
                    totalSteppingNet = totalSteppingNet.add(nodeNetConsumptions.get(sn.getMeterPointId()));
                }

                MeterPointNode primarySteppingNode = steppingNodes.get(0);
                Map<String, Object> operands = new HashMap<>();
                operands.put("NET_KWH", totalSteppingNet);
                operands.put("FAST_TARIFF_CODE", primarySteppingNode.getTariffCode());
                operands.put("NORMS_FACTOR", config.getNormsFactor());
                operands.put("PRO_RATA_FACTOR", proRataFactor);
                operands.put("TARIFFS", config.getTariffs());

                BillingVariant variant = VariantRegistry.get("STEP_RATING");
                variant.execute(operands, ratingStep);

                BigDecimal steppingAmount = (BigDecimal) operands.get(ratingStep.getOutputOperands().get("amount"));
                List<RatingStepEngine.StepResult> steps = (List<RatingStepEngine.StepResult>) operands.get(ratingStep.getOutputOperands().get("breakdown"));

                totalBaseAmount = totalBaseAmount.add(steppingAmount);

                Map<String, Object> primaryBreakdown = new HashMap<>();
                primaryBreakdown.put("meter_point_id", primarySteppingNode.getMeterPointId());
                primaryBreakdown.put("tariff_code", primarySteppingNode.getTariffCode());
                primaryBreakdown.put("net_consumption", totalSteppingNet);
                primaryBreakdown.put("amount", steppingAmount);
                primaryBreakdown.put("steps", steps);
                meterPointBreakdowns.put(primarySteppingNode.getMeterPointId(), primaryBreakdown);

                for (RatingStepEngine.StepResult r : steps) {
                    Map<String, Object> sd = new HashMap<>();
                    sd.put("meter_point_id", primarySteppingNode.getMeterPointId());
                    sd.put("step", r.getStep());
                    sd.put("kwh", r.getKwhConsumed());
                    sd.put("price", r.getUnitPrice());
                    sd.put("amount", r.getAmount());
                    stepDetails.add(sd);
                }

                // Associate other stepping nodes with 0 values to avoid double billing
                for (int i = 1; i < steppingNodes.size(); i++) {
                    MeterPointNode otherNode = steppingNodes.get(i);
                    Map<String, Object> otherBreakdown = new HashMap<>();
                    otherBreakdown.put("meter_point_id", otherNode.getMeterPointId());
                    otherBreakdown.put("tariff_code", otherNode.getTariffCode());
                    otherBreakdown.put("net_consumption", nodeNetConsumptions.get(otherNode.getMeterPointId()));
                    otherBreakdown.put("amount", BigDecimal.ZERO);
                    otherBreakdown.put("steps", Collections.emptyList());
                    meterPointBreakdowns.put(otherNode.getMeterPointId(), otherBreakdown);
                }
            }

            // Execute flat rating calculation separately for each flat node (TOU/manufacturing)
            for (MeterPointNode flatNode : flatNodes) {
                BigDecimal net = nodeNetConsumptions.get(flatNode.getMeterPointId());
                Map<String, Object> operands = new HashMap<>();
                operands.put("NET_KWH", net);
                operands.put("FAST_TARIFF_CODE", flatNode.getTariffCode());
                operands.put("NORMS_FACTOR", config.getNormsFactor());
                operands.put("PRO_RATA_FACTOR", proRataFactor);
                operands.put("TARIFFS", config.getTariffs());

                BillingVariant variant = VariantRegistry.get("FLAT_RATING");
                variant.execute(operands, ratingStep);

                BigDecimal flatAmount = (BigDecimal) operands.get(ratingStep.getOutputOperands().get("amount"));
                List<RatingStepEngine.StepResult> steps = (List<RatingStepEngine.StepResult>) operands.get(ratingStep.getOutputOperands().get("breakdown"));

                totalBaseAmount = totalBaseAmount.add(flatAmount);

                Map<String, Object> nodeBreakdown = new HashMap<>();
                nodeBreakdown.put("meter_point_id", flatNode.getMeterPointId());
                nodeBreakdown.put("tariff_code", flatNode.getTariffCode());
                nodeBreakdown.put("net_consumption", net);
                nodeBreakdown.put("amount", flatAmount);
                nodeBreakdown.put("steps", steps);
                meterPointBreakdowns.put(flatNode.getMeterPointId(), nodeBreakdown);

                for (RatingStepEngine.StepResult r : steps) {
                    Map<String, Object> sd = new HashMap<>();
                    sd.put("meter_point_id", flatNode.getMeterPointId());
                    sd.put("step", r.getStep());
                    sd.put("kwh", r.getKwhConsumed());
                    sd.put("price", r.getUnitPrice());
                    sd.put("amount", r.getAmount());
                    stepDetails.add(sd);
                }
            }
        }

        // 2. Set up Account-Level Operands Context
        Map<String, Object> accountOperands = new HashMap<>();
        accountOperands.put("BASE_AMOUNT", totalBaseAmount);
        accountOperands.put("DISCOUNT_AMOUNT", BigDecimal.ZERO);
        accountOperands.put("TAX_AMOUNT", BigDecimal.ZERO);
        accountOperands.put("TOTAL_AMOUNT", totalBaseAmount);

        // 3. Execute remaining steps in the Billing Schema sequentially (e.g., Discount, Taxes)
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

    private void collectAllNodes(MeterPointNode node, List<MeterPointNode> allNodes) {
        if (node == null) return;
        allNodes.add(node);
        if (node.getChildPoints() != null) {
            for (MeterPointNode child : node.getChildPoints()) {
                collectAllNodes(child, allNodes);
            }
        }
    }
}
