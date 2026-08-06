package com.evn.billing.engine.variant;

import com.evn.billing.common.dto.BillingSchemaStep;
import com.evn.billing.common.dto.MeterPointNode;
import com.evn.billing.common.dto.MeterTopology;
import com.evn.billing.engine.TopologyCalculator;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

public class NettingCalculatorVariant implements BillingVariant {

    private final TopologyCalculator topologyCalculator = new TopologyCalculator();

    @Override
    @SuppressWarnings("unchecked")
    public void execute(Map<String, Object> operands, BillingSchemaStep step) throws Exception {
        // 1. Get input keys from operands map
        String topologyKey = step.getInputOperands().getOrDefault("meterTopology", "METER_TOPOLOGY");
        String consumptionsKey = step.getInputOperands().getOrDefault("consumptions", "RAW_CONSUMPTIONS");

        MeterTopology topology = (MeterTopology) operands.get(topologyKey);
        Map<String, BigDecimal> consumptions = (Map<String, BigDecimal>) operands.get(consumptionsKey);

        if (topology == null || consumptions == null) {
            throw new IllegalArgumentException("NettingCalculatorVariant requires both MeterTopology and Consumptions map in operands.");
        }

        // 2. Perform netting calculation recursively
        Map<String, BigDecimal> netConsumptions = new HashMap<>();
        BigDecimal totalNet = BigDecimal.ZERO;

        if (topology.getRootPoints() != null) {
            for (MeterPointNode root : topology.getRootPoints()) {
                BigDecimal rootNet = calculateAndCollect(root, consumptions, netConsumptions);
                totalNet = totalNet.add(rootNet);
            }
        }

        // 3. Store result back into operands
        String netConsumptionsOutKey = step.getOutputOperands().getOrDefault("netConsumptions", "NET_CONSUMPTIONS");
        String totalNetKwhOutKey = step.getOutputOperands().getOrDefault("totalNetKwh", "TOTAL_NET_KWH");

        operands.put(netConsumptionsOutKey, netConsumptions);
        operands.put(totalNetKwhOutKey, totalNet);
    }

    private BigDecimal calculateAndCollect(MeterPointNode node, Map<String, BigDecimal> consumptions, Map<String, BigDecimal> netConsumptions) {
        BigDecimal netValue = topologyCalculator.calculateNetConsumption(node, consumptions);
        netConsumptions.put(node.getMaDdo(), netValue);
        
        if (node.getChildPoints() != null) {
            for (MeterPointNode child : node.getChildPoints()) {
                calculateAndCollect(child, consumptions, netConsumptions);
            }
        }
        return netValue;
    }
}
