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
        b1.setSoThuTu(1); b1.setMinKwh(BigDecimal.ZERO); b1.setMaxKwh(BigDecimal.valueOf(50.0)); b1.setDonGia(BigDecimal.valueOf(1806));
        standardBlocks.add(b1);

        TariffBlock b2 = new TariffBlock();
        b2.setSoThuTu(2); b2.setMinKwh(BigDecimal.valueOf(50.0)); b2.setMaxKwh(BigDecimal.valueOf(100.0)); b2.setDonGia(BigDecimal.valueOf(1866));
        standardBlocks.add(b2);

        TariffBlock b3 = new TariffBlock();
        b3.setSoThuTu(3); b3.setMinKwh(BigDecimal.valueOf(100.0)); b3.setMaxKwh(BigDecimal.valueOf(200.0)); b3.setDonGia(BigDecimal.valueOf(2167));
        standardBlocks.add(b3);

        TariffBlock b4 = new TariffBlock();
        b4.setSoThuTu(4); b4.setMinKwh(BigDecimal.valueOf(200.0)); b4.setMaxKwh(BigDecimal.valueOf(300.0)); b4.setDonGia(BigDecimal.valueOf(2729));
        standardBlocks.add(b4);

        TariffBlock b5 = new TariffBlock();
        b5.setSoThuTu(5); b5.setMinKwh(BigDecimal.valueOf(300.0)); b5.setMaxKwh(BigDecimal.valueOf(400.0)); b5.setDonGia(BigDecimal.valueOf(3050));
        standardBlocks.add(b5);

        TariffBlock b6 = new TariffBlock();
        b6.setSoThuTu(6); b6.setMinKwh(BigDecimal.valueOf(400.0)); b6.setMaxKwh(null); b6.setDonGia(BigDecimal.valueOf(3157));
        standardBlocks.add(b6);
    }

    @Test
    public void testTopologyCalculationWithNetting() {
        // Construct topology: Root (METER-01) netting Child (METER-02)
        MeterPointNode root = new MeterPointNode();
        root.setMaDdo("METER-01");
        root.setCalculationType(CalculationType.AGGREGATION);

        MeterPointNode child = new MeterPointNode();
        child.setMaDdo("METER-02");
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

    @Test
    public void testCustomerClassificationCases() throws Exception {
        BillingCalculator calculator = new BillingCalculator();

        // 1. Setup default tariffs map
        Map<String, com.evn.billing.common.dto.TariffRules> tariffs = new HashMap<>();
        
        com.evn.billing.common.dto.TariffRules shbtRules = new com.evn.billing.common.dto.TariffRules();
        shbtRules.setMaNgia("TARIFF_SHBT_2023");
        shbtRules.setLoaiBieuGia("STEPPING");
        shbtRules.setBlocks(standardBlocks);
        tariffs.put("TARIFF_SHBT_2023", shbtRules);

        com.evn.billing.common.dto.TariffRules flatRules = new com.evn.billing.common.dto.TariffRules();
        flatRules.setMaNgia("TARIFF_NGOAI_SH");
        flatRules.setLoaiBieuGia("FLAT");
        TariffBlock flatBlock = new TariffBlock();
        flatBlock.setSoThuTu(1); flatBlock.setMinKwh(BigDecimal.ZERO); flatBlock.setMaxKwh(null); flatBlock.setDonGia(BigDecimal.valueOf(2500));
        flatRules.setBlocks(List.of(flatBlock));
        tariffs.put("TARIFF_NGOAI_SH", flatRules);

        // Standard Schema Steps (STEP_RATING & FLAT_RATING)
        List<com.evn.billing.common.dto.BillingSchemaStep> schemaSteps = new ArrayList<>();
        com.evn.billing.common.dto.BillingSchemaStep ratingStep = new com.evn.billing.common.dto.BillingSchemaStep();
        ratingStep.setStepNumber(10);
        ratingStep.setVariantName("STEP_RATING");
        ratingStep.setInputOperands(Map.of("consumption", "NET_KWH", "tariffCode", "FAST_TARIFF_CODE", "norms", "NORMS_FACTOR", "proRata", "PRO_RATA_FACTOR"));
        ratingStep.setOutputOperands(Map.of("amount", "AMOUNT_OUT", "breakdown", "BREAKDOWN_OUT"));
        schemaSteps.add(ratingStep);

        // 2. Case 1: SINH_HOAT_DU_DINH_MUC
        com.evn.billing.common.dto.BillingConfigSnapshot config1 = new com.evn.billing.common.dto.BillingConfigSnapshot();
        config1.setMaKhang("KH-01");
        config1.setNormsFactor(1);
        config1.setCustomerType("SINH_HOAT");
        config1.setBieuGia(tariffs);
        config1.setSchemaSteps(schemaSteps);

        com.evn.billing.common.dto.MeterTopology topology1 = new com.evn.billing.common.dto.MeterTopology();
        MeterPointNode node1 = new MeterPointNode();
        node1.setMaDdo("MP-01");
        node1.setCalculationType(CalculationType.AGGREGATION);
        node1.setMaNgia("TARIFF_SHBT_2023");
        topology1.setRootPoints(List.of(node1));
        config1.setMeterTopology(topology1);

        Map<String, BigDecimal> cons1 = Map.of("MP-01", BigDecimal.valueOf(350));
        CalculationResult res1 = calculator.calculate(config1, cons1, "2026_06", 30);
        Map<String, Object> br1 = (Map<String, Object>) res1.getMeterPointBreakdowns().get("MP-01");
        assertEquals("SINH_HOAT_DU_DINH_MUC", br1.get("customer_case"));

        // 3. Case 2: SINH_HOAT_THIEU_DINH_MUC (Short prorated days)
        CalculationResult res2 = calculator.calculate(config1, cons1, "2026_06", 15);
        Map<String, Object> br2 = (Map<String, Object>) res2.getMeterPointBreakdowns().get("MP-01");
        assertEquals("SINH_HOAT_THIEU_DINH_MUC", br2.get("customer_case"));

        // 4. Case 3: NGOAI_SINH_HOAT_100
        node1.setMaNgia("TARIFF_NGOAI_SH");
        config1.setCustomerType("NGOAI_SINH_HOAT");
        CalculationResult res3 = calculator.calculate(config1, cons1, "2026_06", 30);
        Map<String, Object> br3 = (Map<String, Object>) res3.getMeterPointBreakdowns().get("MP-01");
        assertEquals("NGOAI_SINH_HOAT_100", br3.get("customer_case"));

        // 5. Case 4: NGOAI_SINH_HOAT_MIX_SHBT_TL (Mixed split by ratio)
        com.evn.billing.common.dto.PriceApplicationRule r1 = new com.evn.billing.common.dto.PriceApplicationRule();
        r1.setMaNgia("TARIFF_NGOAI_SH");
        r1.setLoaiDmuc("TL");
        r1.setDinhMuc(BigDecimal.valueOf(70));
        r1.setSoHo(1);

        com.evn.billing.common.dto.PriceApplicationRule r2 = new com.evn.billing.common.dto.PriceApplicationRule();
        r2.setMaNgia("TARIFF_SHBT_2023");
        r2.setLoaiDmuc("TL");
        r2.setDinhMuc(BigDecimal.valueOf(30));
        r2.setSoHo(1);

        node1.setPriceRules(List.of(r1, r2));
        config1.setCustomerType("MIXED");
        CalculationResult res4 = calculator.calculate(config1, cons1, "2026_06", 30);
        Map<String, Object> br4 = (Map<String, Object>) res4.getMeterPointBreakdowns().get("MP-01");
        assertEquals("NGOAI_SINH_HOAT_MIX_SHBT_TL", br4.get("customer_case"));

        // 6. Case 5: NGOAI_SINH_HOAT_MIX_SHBT_SL (Mixed split by absolute volume limit)
        r1.setLoaiDmuc("SL");
        r1.setDinhMuc(BigDecimal.valueOf(150));
        r2.setLoaiDmuc("SL");
        r2.setDinhMuc(BigDecimal.valueOf(99999));

        CalculationResult res5 = calculator.calculate(config1, cons1, "2026_06", 30);
        Map<String, Object> br5 = (Map<String, Object>) res5.getMeterPointBreakdowns().get("MP-01");
        assertEquals("NGOAI_SINH_HOAT_MIX_SHBT_SL", br5.get("customer_case"));
    }

    @Test
    public void testExpressionEvalVariant() throws Exception {
        com.evn.billing.engine.variant.ExpressionEvalVariant evalVariant = new com.evn.billing.engine.variant.ExpressionEvalVariant();

        // Test 1: Multiplication
        com.evn.billing.common.dto.BillingSchemaStep step1 = new com.evn.billing.common.dto.BillingSchemaStep();
        step1.setStepConfig(Map.of("expression", "operands['BASE_AMOUNT'] * 0.10"));
        step1.setOutputOperands(Map.of("result", "DISCOUNT_AMOUNT"));

        Map<String, Object> operands = new HashMap<>();
        operands.put("BASE_AMOUNT", BigDecimal.valueOf(1000));
        
        evalVariant.execute(operands, step1);
        BigDecimal discount = (BigDecimal) operands.get("DISCOUNT_AMOUNT");
        assertEquals(0, discount.compareTo(BigDecimal.valueOf(100)));

        // Test 2: Conditional
        com.evn.billing.common.dto.BillingSchemaStep step2 = new com.evn.billing.common.dto.BillingSchemaStep();
        step2.setStepConfig(Map.of("expression", "operands['TOTAL_AMOUNT'] > 1000000 ? 50000 : 0"));
        step2.setOutputOperands(Map.of("result", "POST_INVOICE_DISCOUNT"));

        operands.put("TOTAL_AMOUNT", BigDecimal.valueOf(1200000));
        evalVariant.execute(operands, step2);
        BigDecimal disc = (BigDecimal) operands.get("POST_INVOICE_DISCOUNT");
        assertEquals(0, disc.compareTo(BigDecimal.valueOf(50000)));

        operands.put("TOTAL_AMOUNT", BigDecimal.valueOf(800000));
        evalVariant.execute(operands, step2);
        disc = (BigDecimal) operands.get("POST_INVOICE_DISCOUNT");
        assertEquals(0, disc.compareTo(BigDecimal.ZERO));
    }
}
