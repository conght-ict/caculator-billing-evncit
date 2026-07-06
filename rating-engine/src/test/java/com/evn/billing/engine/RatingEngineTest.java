package com.evn.billing.engine;

import com.evn.billing.common.dto.CalculationType;
import com.evn.billing.common.dto.MeterPointNode;
import com.evn.billing.common.dto.TariffBlock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class RatingEngineTest {

    private TopologyCalculator topologyCalculator;
    private RatingStepEngine ratingStepEngine;
    private List<TariffBlock> standardBlocks;

    @BeforeEach
    public void setUp() {
        topologyCalculator = new TopologyCalculator();
        ratingStepEngine = new RatingStepEngine();

        // Standard EVN stepping blocks config
        standardBlocks = new ArrayList<>();
        
        TariffBlock b1 = new TariffBlock();
        b1.setStep(1); b1.setMinKwh(0); b1.setMaxKwh(50.0); b1.setUnitPrice(1806);
        standardBlocks.add(b1);

        TariffBlock b2 = new TariffBlock();
        b2.setStep(2); b2.setMinKwh(50.0); b2.setMaxKwh(100.0); b2.setUnitPrice(1866);
        standardBlocks.add(b2);

        TariffBlock b3 = new TariffBlock();
        b3.setStep(3); b3.setMinKwh(100.0); b3.setMaxKwh(200.0); b3.setUnitPrice(2167);
        standardBlocks.add(b3);

        TariffBlock b4 = new TariffBlock();
        b4.setStep(4); b4.setMinKwh(200.0); b4.setMaxKwh(300.0); b4.setUnitPrice(2729);
        standardBlocks.add(b4);

        TariffBlock b5 = new TariffBlock();
        b5.setStep(5); b5.setMinKwh(300.0); b5.setMaxKwh(400.0); b5.setUnitPrice(3050);
        standardBlocks.add(b5);

        TariffBlock b6 = new TariffBlock();
        b6.setStep(6); b6.setMinKwh(400.0); b6.setMaxKwh(null); b6.setUnitPrice(3157);
        standardBlocks.add(b6);
    }

    @Test
    public void testTopologyCalculationWithNetting() {
        // Construct topology: Root (METER-01) netting Child (METER-02)
        MeterPointNode root = new MeterPointNode();
        root.setMeterPointId("METER-01");
        root.setCalculationType(CalculationType.AGGREGATION);

        MeterPointNode child = new MeterPointNode();
        child.setMeterPointId("METER-02");
        child.setCalculationType(CalculationType.NETTING);

        root.setChildPoints(List.of(child));

        // Case 1: normal netting
        Map<String, BigDecimal> consumptions1 = new HashMap<>();
        consumptions1.put("METER-01", BigDecimal.valueOf(500.00));
        consumptions1.put("METER-02", BigDecimal.valueOf(50.00));

        BigDecimal net1 = topologyCalculator.calculateNetConsumption(root, consumptions1);
        assertEquals(0, net1.compareTo(BigDecimal.valueOf(450.00)));

        // Case 2: negative subtraction safeguard
        Map<String, BigDecimal> consumptions2 = new HashMap<>();
        consumptions2.put("METER-01", BigDecimal.valueOf(100.00));
        consumptions2.put("METER-02", BigDecimal.valueOf(150.00));

        BigDecimal net2 = topologyCalculator.calculateNetConsumption(root, consumptions2);
        assertEquals(0, net2.compareTo(BigDecimal.ZERO));
    }

    @Test
    public void testSteppingTariffNormsFactor1() {
        // Consumption = 250 kWh, Norms = 1 (Standard)
        BigDecimal consumption = BigDecimal.valueOf(250.0);
        List<RatingStepEngine.StepResult> results = ratingStepEngine.calculateSteppingTariff(consumption, standardBlocks, 1);

        assertEquals(4, results.size());
        
        // Tier 1: 50 kWh * 1806 = 90,300
        assertEquals(50.0, results.get(0).getKwhConsumed().doubleValue());
        assertEquals(90300.0, results.get(0).getAmount().doubleValue());

        // Tier 2: 50 kWh * 1866 = 93,300
        assertEquals(50.0, results.get(1).getKwhConsumed().doubleValue());
        assertEquals(93300.0, results.get(1).getAmount().doubleValue());

        // Tier 3: 100 kWh * 2167 = 216,700
        assertEquals(100.0, results.get(2).getKwhConsumed().doubleValue());
        assertEquals(216700.0, results.get(2).getAmount().doubleValue());

        // Tier 4: 50 kWh * 2729 = 136,450
        assertEquals(50.0, results.get(3).getKwhConsumed().doubleValue());
        assertEquals(136450.0, results.get(3).getAmount().doubleValue());
    }

    @Test
    public void testSteppingTariffNormsFactor3() {
        // Consumption = 250 kWh, Norms = 3 (Shared Household)
        BigDecimal consumption = BigDecimal.valueOf(250.0);
        List<RatingStepEngine.StepResult> results = ratingStepEngine.calculateSteppingTariff(consumption, standardBlocks, 3);

        // Scaled tiers width:
        // Tier 1 limit: 50 * 3 = 150 kWh
        // Tier 2 limit: (100 - 50) * 3 = 150 kWh
        // Total consumption (250) falls into Tier 1 (150) and Tier 2 (100)
        assertEquals(2, results.size());

        // Tier 1: 150 kWh * 1806 = 270,900
        assertEquals(150.0, results.get(0).getKwhConsumed().doubleValue());
        assertEquals(270900.0, results.get(0).getAmount().doubleValue());

        // Tier 2: 100 kWh * 1866 = 186,600
        assertEquals(100.0, results.get(1).getKwhConsumed().doubleValue());
        assertEquals(186600.0, results.get(1).getAmount().doubleValue());
    }

    @Test
    public void testSteppingTariffProRataScaling() {
        // Consumption = 125.0 kWh, Norms = 1, Pro-rata factor = 0.5 (e.g. 15 days out of 30)
        BigDecimal consumption = BigDecimal.valueOf(125.0);
        List<RatingStepEngine.StepResult> results = ratingStepEngine.calculateSteppingTariff(
                consumption, standardBlocks, 1, BigDecimal.valueOf(0.5));

        // Scaled tiers width with pro-rata = 0.5:
        // Tier 1 limit: 50 * 0.5 = 25 kWh
        // Tier 2 limit: 50 * 0.5 = 25 kWh
        // Tier 3 limit: 100 * 0.5 = 50 kWh
        // Tier 4 limit: 100 * 0.5 = 50 kWh
        // Total consumption (125) falls into Tier 1 (25), Tier 2 (25), Tier 3 (50), Tier 4 (25)
        assertEquals(4, results.size());

        // Tier 1: 25 kWh * 1806 = 45,150
        assertEquals(25.0, results.get(0).getKwhConsumed().doubleValue());
        assertEquals(45150.0, results.get(0).getAmount().doubleValue());

        // Tier 2: 25 kWh * 1866 = 46,650
        assertEquals(25.0, results.get(1).getKwhConsumed().doubleValue());
        assertEquals(46650.0, results.get(1).getAmount().doubleValue());

        // Tier 3: 50 kWh * 2167 = 108,350
        assertEquals(50.0, results.get(2).getKwhConsumed().doubleValue());
        assertEquals(108350.0, results.get(2).getAmount().doubleValue());

        // Tier 4: 25 kWh * 2729 = 68,225
        assertEquals(25.0, results.get(3).getKwhConsumed().doubleValue());
        assertEquals(68225.0, results.get(3).getAmount().doubleValue());
    }
}
