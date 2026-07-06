package com.evn.billing.engine;

import com.evn.billing.common.dto.MeterPointNode;
import com.evn.billing.common.dto.CalculationType;
import java.math.BigDecimal;
import java.util.Map;

public class TopologyCalculator {

    /**
     * Calculates the net consumption of a meter node by recursively traversing its children.
     * AGGREGATION: Child consumption is added to the node.
     * NETTING: Child consumption is subtracted from the node.
     * 
     * @param node The current meter topology node
     * @param consumptions A map containing raw consumption values for each meter_point_id
     * @return The calculated Net Consumption (never negative)
     */
    public BigDecimal calculateNetConsumption(MeterPointNode node, Map<String, BigDecimal> consumptions) {
        BigDecimal nodeRaw = consumptions.getOrDefault(node.getMeterPointId(), BigDecimal.ZERO);
        BigDecimal netValue = nodeRaw;

        if (node.getChildPoints() != null) {
            for (MeterPointNode child : node.getChildPoints()) {
                BigDecimal childNet = calculateNetConsumption(child, consumptions);
                
                if (child.getCalculationType() == CalculationType.AGGREGATION) {
                    netValue = netValue.add(childNet);
                } else if (child.getCalculationType() == CalculationType.NETTING) {
                    netValue = netValue.subtract(childNet);
                }
            }
        }
        
        // Return 0 if the netting calculations result in a negative number
        return netValue.compareTo(BigDecimal.ZERO) < 0 ? BigDecimal.ZERO : netValue;
    }
}
