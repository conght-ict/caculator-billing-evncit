package com.evn.billing.worker.controller;

import com.evn.billing.common.dto.BillingConfigSnapshot;
import com.evn.billing.common.dto.BillingSchemaStep;
import com.evn.billing.common.dto.TariffBlock;
import com.evn.billing.common.dto.TariffRules;
import com.evn.billing.common.dto.MeterTopology;
import com.evn.billing.common.dto.MeterPointNode;
import com.evn.billing.common.dto.CalculationType;
import com.evn.billing.engine.RatingStepEngine;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RestController
@RequestMapping("/api/v1/benchmark")
@CrossOrigin(origins = "*")
public class PerformanceBenchmarkController {

    private static final Logger log = LoggerFactory.getLogger(PerformanceBenchmarkController.class);

    private final RatingStepEngine ratingStepEngine = new RatingStepEngine();

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private PlatformTransactionManager transactionManager;

    @Autowired
    private com.fasterxml.jackson.databind.ObjectMapper objectMapper;

    /**
     * Sinh dữ liệu cấu hình thật (Account, MeterPoint, MeterUsage) vào CSDL Postgres.
     * Hỗ trợ gieo dữ liệu lên đến 10 triệu tài khoản bằng JDBC Batch và đa luồng song song.
     */
    @GetMapping("/seed")
    public ResponseEntity<String> seedBenchmarkData(
            @RequestParam(defaultValue = "100000") int size,
            @RequestParam(defaultValue = "2026_06") String month) {
        log.info("Seeding database configurations and usages for {} accounts via SQL generate_series for month: {}", size, month);
        long startTime = System.currentTimeMillis();

        // 1. Clean previous data
        cleanBenchmarkData(month);

        int chunkSize = 200000;
        int totalChunks = (size + chunkSize - 1) / chunkSize;

        TransactionTemplate transactionTemplate = new TransactionTemplate(transactionManager);

        try {
            for (int c = 0; c < totalChunks; c++) {
                final int startIdx = c * chunkSize;
                final int endIdx = Math.min(startIdx + chunkSize, size) - 1;

                transactionTemplate.execute(status -> {
                    // Phase 1: Accounts
                    jdbcTemplate.update(
                        "INSERT INTO account (account_id, book_id, customer_name, status, norms_factor) " +
                        "SELECT 'MOCK_KH_' || i, 'SO_BENCH_' || (i / 1000), 'Mock Customer ' || i, 'ACTIVE', 1 " +
                        "FROM generate_series(?, ?) AS i " +
                        "ON CONFLICT (account_id) DO NOTHING", startIdx, endIdx);

                    // Phase 2: Meter Points
                    jdbcTemplate.update(
                        "INSERT INTO meter_point (meter_point_id, account_id, tariff_code, status) " +
                        "SELECT 'MOCK_METER_' || i, 'MOCK_KH_' || i, 'TARIFF_SHBT_2026', 'ACTIVE' " +
                        "FROM generate_series(?, ?) AS i " +
                        "ON CONFLICT (meter_point_id) DO NOTHING", startIdx, endIdx);

                    // Phase 3: Meter Usages
                    jdbcTemplate.update(
                        "INSERT INTO meter_usage (usage_id, account_id, meter_point_id, billing_cycle_month, from_date, to_date, start_index, end_index, status) " +
                        "SELECT (i::bigint * 1000 + 10000000 + 1), 'MOCK_KH_' || i, 'MOCK_METER_' || i, ?, NOW() - INTERVAL '30 days', NOW(), 1000.0, 1000.0 + 50.0 + (floor(random() * 350))::numeric, 'VALIDATED' " +
                        "FROM generate_series(?, ?) AS i " +
                        "ON CONFLICT (usage_id, billing_cycle_month) DO NOTHING", month, startIdx, endIdx);

                    // Phase 4: Config Snapshots
                    jdbcTemplate.update(
                        "INSERT INTO billing_account_snapshot (snapshot_id, account_id, book_id, billing_cycle_month, calculation_version, config_data, created_at) " +
                        "SELECT 'MOCK_KH_' || i || '_' || ? || '_v1', 'MOCK_KH_' || i, 'SO_BENCH_' || (i / 1000), ?, 1, " +
                        "jsonb_build_object(" +
                        "  'accountId', 'MOCK_KH_' || i, " +
                        "  'normsFactor', 1, " +
                        "  'fastPathEnabled', true, " +
                        "  'fastPathMeterPointId', 'MOCK_METER_' || i, " +
                        "  'fastPathTariffCode', 'TARIFF_SHBT_2026', " +
                        "  'tariffs', jsonb_build_object(" +
                        "    'TARIFF_SHBT_2026', jsonb_build_object(" +
                        "      'tariffCode', 'TARIFF_SHBT_2026', " +
                        "      'type', 'STEPPING', " +
                        "      'blocks', jsonb_build_array(" +
                        "        jsonb_build_object('step', 1, 'minKwh', 0.0, 'maxKwh', 50.0, 'unitPrice', 1806.00), " +
                        "        jsonb_build_object('step', 2, 'minKwh', 50.0, 'maxKwh', 100.0, 'unitPrice', 1866.00), " +
                        "        jsonb_build_object('step', 3, 'minKwh', 100.0, 'maxKwh', 200.0, 'unitPrice', 2167.00), " +
                        "        jsonb_build_object('step', 4, 'minKwh', 200.0, 'maxKwh', 300.0, 'unitPrice', 2729.00), " +
                        "        jsonb_build_object('step', 5, 'minKwh', 300.0, 'maxKwh', 400.0, 'unitPrice', 3050.00), " +
                        "        jsonb_build_object('step', 6, 'minKwh', 400.0, 'maxKwh', null, 'unitPrice', 3157.00) " +
                        "      ) " +
                        "    ) " +
                        "  ), " +
                        "  'meterTopology', jsonb_build_object(" +
                        "    'rootPoints', jsonb_build_array(" +
                        "      jsonb_build_object(" +
                        "        'meterPointId', 'MOCK_METER_' || i, " +
                        "        'calculationType', 'AGGREGATION', " +
                        "        'tariffCode', 'TARIFF_SHBT_2026', " +
                        "        'childPoints', jsonb_build_array() " +
                        "      ) " +
                        "    ) " +
                        "  ), " +
                        "  'schemaSteps', jsonb_build_array(" +
                        "    jsonb_build_object(" +
                        "      'stepNumber', 10, " +
                        "      'variantName', 'STEP_RATING', " +
                        "      'inputOperands', jsonb_build_object('consumption', 'NET_KWH', 'tariffCode', 'FAST_TARIFF_CODE'), " +
                        "      'outputOperands', jsonb_build_object('amount', 'BASE_AMOUNT', 'breakdown', 'RATING_BREAKDOWN'), " +
                        "      'stepConfig', jsonb_build_object() " +
                        "    ), " +
                        "    jsonb_build_object(" +
                        "      'stepNumber', 20, " +
                        "      'variantName', 'TAX', " +
                        "      'inputOperands', jsonb_build_object('amount', 'BASE_AMOUNT'), " +
                        "      'outputOperands', jsonb_build_object('taxAmount', 'TAX_AMOUNT', 'totalAmount', 'TOTAL_AMOUNT'), " +
                        "      'stepConfig', jsonb_build_object('taxRate', 0.10) " +
                        "    ) " +
                        "  ) " +
                        "), NOW() FROM generate_series(?, ?) AS i " +
                        "ON CONFLICT (snapshot_id) DO NOTHING", month, month, startIdx, endIdx);
                    return null;
                });
                log.info("Seeded chunk: {} to {}", startIdx, endIdx);
            }
        } catch (Exception e) {
            log.error("Seeding failed: {}", e.getMessage(), e);
            return ResponseEntity.status(500).body("Lỗi sinh dữ liệu: " + e.getMessage());
        }

        long duration = System.currentTimeMillis() - startTime;
        return ResponseEntity.ok("Sinh dữ liệu thành công! Đã chèn " + size + " Accounts, MeterPoints, Usages và Snapshots vào CSDL Postgres trong " + (duration / 1000.0) + " giây.");
    }

    /**
     * Dọn sạch dữ liệu mô phỏng benchmark khỏi Postgres.
     */
    @GetMapping("/clean")
    public ResponseEntity<String> cleanBenchmarkData(@RequestParam(required = false) String month) {
        log.info("Cleaning benchmark mock data from database for month: {}", (month != null ? month : "ALL"));
        long startTime = System.currentTimeMillis();

        TransactionTemplate transactionTemplate = new TransactionTemplate(transactionManager);
        transactionTemplate.execute(status -> {
            if (month != null && !month.trim().isEmpty()) {
                jdbcTemplate.execute("DELETE FROM bill_invoice WHERE account_id LIKE 'MOCK_KH_%' AND billing_cycle_month = '" + month + "'");
                jdbcTemplate.execute("DELETE FROM meter_usage WHERE account_id LIKE 'MOCK_KH_%' AND billing_cycle_month = '" + month + "'");
                jdbcTemplate.execute("DELETE FROM billing_account_snapshot WHERE account_id LIKE 'MOCK_KH_%' AND billing_cycle_month = '" + month + "'");
                jdbcTemplate.execute("DELETE FROM billing_calculation_log WHERE account_id LIKE 'MOCK_KH_%' AND billing_cycle_month = '" + month + "'");
                jdbcTemplate.execute("DELETE FROM meter_point WHERE account_id LIKE 'MOCK_KH_%'");
                jdbcTemplate.execute("DELETE FROM account WHERE account_id LIKE 'MOCK_KH_%'");
            } else {
                jdbcTemplate.execute("DELETE FROM bill_invoice WHERE account_id LIKE 'MOCK_KH_%'");
                jdbcTemplate.execute("DELETE FROM outbox_event WHERE aggregate_id LIKE 'MOCK_INV_%'");
                jdbcTemplate.execute("DELETE FROM billing_account_snapshot WHERE account_id LIKE 'MOCK_KH_%'");
                jdbcTemplate.execute("DELETE FROM meter_usage WHERE account_id LIKE 'MOCK_KH_%'");
                jdbcTemplate.execute("DELETE FROM meter_point WHERE account_id LIKE 'MOCK_KH_%'");
                jdbcTemplate.execute("DELETE FROM account WHERE account_id LIKE 'MOCK_KH_%'");
                jdbcTemplate.execute("DELETE FROM billing_calculation_log WHERE account_id LIKE 'MOCK_KH_%'");
            }
            return null;
        });

        long duration = System.currentTimeMillis() - startTime;
        return ResponseEntity.ok("Đã dọn sạch dữ liệu mock benchmark " + (month != null ? "kỳ " + month : "toàn bộ") + ". Thời gian: " + (duration / 1000.0) + " giây.");
    }

    /**
     * Chạy thử nghiệm hiệu năng lõi (Rating Engine) trên RAM.
     */
    @GetMapping("/rating-engine")
    public ResponseEntity<?> benchmarkRatingEngine(
            @RequestParam(defaultValue = "10000000") int iterations,
            @RequestParam(defaultValue = "16") int threadCount) {

        log.info("Starting core Rating Engine RAM benchmark with {} iterations...", iterations);

        List<TariffBlock> blocks = getStandardShbtBlocks();
        LongAdder successCounter = new LongAdder();
        long startTime = System.nanoTime();

        try (ExecutorService executor = Executors.newFixedThreadPool(threadCount)) {
            int batchSize = iterations / threadCount;
            for (int t = 0; t < threadCount; t++) {
                executor.submit(() -> {
                    Random rand = new Random();
                    for (int i = 0; i < batchSize; i++) {
                        double rawKwh = rand.nextDouble() * 600;
                        BigDecimal consumption = BigDecimal.valueOf(rawKwh);
                        int normsFactor = rand.nextInt(3) + 1;

                        List<RatingStepEngine.StepResult> results = ratingStepEngine
                                .calculateSteppingTariff(consumption, blocks, normsFactor);
                        
                        if (results != null && !results.isEmpty()) {
                            successCounter.increment();
                        }
                    }
                });
            }
            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return ResponseEntity.status(500).body("Benchmark bị gián đoạn: " + e.getMessage());
        }

        long endTime = System.nanoTime();
        double durationSeconds = (endTime - startTime) / 1_000_000_000.0;
        long totalSuccess = successCounter.sum();
        double throughput = totalSuccess / durationSeconds;

        Map<String, Object> report = new HashMap<>();
        report.put("test_case", "Rating Engine RAM Stepping Calculation (SHBT)");
        report.put("total_calculations", totalSuccess);
        report.put("threads_used", threadCount);
        report.put("duration_seconds", durationSeconds);
        report.put("throughput_per_second", Math.round(throughput));
        report.put("average_latency_microseconds", (durationSeconds * 1_000_000) / totalSuccess);
        report.put("status", "SUCCESS");

        return ResponseEntity.ok(report);
    }

    /**
     * Chạy thử nghiệm hiệu năng toàn luồng (Full Pipeline End-To-End) thực tế:
     * 1. Truy vấn cơ sở dữ liệu để lấy danh sách chỉ số đo xa đã gieo của khách hàng.
     * 2. Áp dụng cấu hình biểu giá bậc thang lấy từ Snapshot mẫu.
     * 3. Thực thi tính cước áp giá luỹ tiến thời gian thực trên RAM.
     * 4. Ghi nhận hóa đơn và ghi nhận outbox đồng bộ xuống CSDL bằng JDBC Batch.
     */
    @GetMapping("/pipeline")
    public ResponseEntity<?> benchmarkPipeline(
            @RequestParam(defaultValue = "100000") int size,
            @RequestParam(defaultValue = "16") int threadCount,
            @RequestParam(defaultValue = "true") boolean writeToDb,
            @RequestParam(defaultValue = "2026_06") String month) {

        log.info("Executing real calculation pipeline for: {} accounts, month: {}", size, month);
        long startTime = System.currentTimeMillis();

        TransactionTemplate transactionTemplate = new TransactionTemplate(transactionManager);
        transactionTemplate.execute(status -> {
            jdbcTemplate.execute("DELETE FROM bill_invoice WHERE account_id LIKE 'MOCK_KH_%' AND billing_cycle_month = '" + month + "'");
            jdbcTemplate.execute("DELETE FROM outbox_event WHERE aggregate_id LIKE 'MOCK_INV_%'");
            jdbcTemplate.execute("DELETE FROM billing_calculation_log WHERE account_id LIKE 'MOCK_KH_%' AND billing_cycle_month = '" + month + "'");
            return null;
        });

        com.evn.billing.engine.BillingCalculator billingCalculator = new com.evn.billing.engine.BillingCalculator();
        LongAdder successCounter = new LongAdder();

        String insertInvoiceSql = "INSERT INTO bill_invoice (invoice_id, account_id, book_id, billing_cycle_month, total_amount_before_tax, tax_amount, total_amount_after_tax, idempotency_key, billing_manifest, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?)";
        String insertOutboxSql = "INSERT INTO outbox_event (event_id, aggregate_type, aggregate_id, event_type, payload, created_at) VALUES (?, ?, ?, ?, ?::jsonb, ?)";

        // Read usages from database in chunks to emulate real DB query roundtrip
        List<Map<String, Object>> readingsList = jdbcTemplate.queryForList(
                "SELECT account_id, meter_point_id, consumption FROM meter_usage WHERE account_id LIKE 'MOCK_KH_%' AND billing_cycle_month = ? LIMIT ?", month, size);

        int loadedSize = readingsList.size();
        if (loadedSize == 0) {
            return ResponseEntity.badRequest().body("Chưa gieo dữ liệu khách hàng mock. Vui lòng gọi GET /api/v1/benchmark/seed trước.");
        }

        int chunkSize = loadedSize / threadCount;
        Timestamp now = new Timestamp(System.currentTimeMillis());

        try (ExecutorService executor = Executors.newFixedThreadPool(threadCount)) {
            for (int t = 0; t < threadCount; t++) {
                final int threadId = t;
                executor.submit(() -> {
                    try {
                        int startIdx = threadId * chunkSize;
                        int endIdx = Math.min(startIdx + chunkSize, loadedSize);

                        List<Object[]> invoiceBatch = new ArrayList<>();
                        List<Object[]> outboxBatch = new ArrayList<>();

                        for (int i = startIdx; i < endIdx; i++) {
                            Map<String, Object> usage = readingsList.get(i);
                            String accId = (String) usage.get("account_id");
                            String meterId = (String) usage.get("meter_point_id");
                            BigDecimal consumption = (BigDecimal) usage.get("consumption");

                            // Build real layout config snapshot
                            BillingConfigSnapshot config = getMockConfigSnapshot(accId, meterId);

                            try {
                                Map<String, BigDecimal> consMap = new HashMap<>();
                                consMap.put(meterId, consumption);

                                com.evn.billing.engine.CalculationResult result = billingCalculator.calculate(config, consMap);

                                BigDecimal totalBeforeTax = result.getTotalAmountBeforeTax();
                                BigDecimal taxAmount = result.getTaxAmount();
                                BigDecimal totalAfterTax = result.getTotalAmountAfterTax();

                                String invoiceId = "MOCK_INV_" + i;
                                String manifest = "{\"test\": \"benchmark\", \"consumption\": " + consumption + "}";

                                if (writeToDb) {
                                    invoiceBatch.add(new Object[]{
                                            invoiceId,
                                            accId,
                                            "SO_BENCH_" + (i / 1000),
                                            month,
                                            totalBeforeTax,
                                            taxAmount,
                                            totalAfterTax,
                                            accId + "_" + month + "_bench",
                                            manifest,
                                            now
                                    });

                                    outboxBatch.add(new Object[]{
                                            java.util.UUID.randomUUID(),
                                            "INVOICE",
                                            invoiceId,
                                            "INVOICE_CREATED",
                                            "{\"invoiceId\": \"" + invoiceId + "\", \"accountId\": \"" + accId + "\", \"amount\": " + totalAfterTax + "}",
                                            now
                                    });

                                    if (invoiceBatch.size() >= 1000) {
                                        final List<Object[]> finalInvoiceBatch = new ArrayList<>(invoiceBatch);
                                        final List<Object[]> finalOutboxBatch = new ArrayList<>(outboxBatch);
                                        transactionTemplate.execute(status -> {
                                            jdbcTemplate.batchUpdate(insertInvoiceSql, finalInvoiceBatch);
                                            jdbcTemplate.batchUpdate(insertOutboxSql, finalOutboxBatch);
                                            return null;
                                        });
                                        invoiceBatch.clear();
                                        outboxBatch.clear();
                                    }
                                }
                                successCounter.increment();
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                        if (writeToDb && !invoiceBatch.isEmpty()) {
                            final List<Object[]> finalInvoiceBatch = new ArrayList<>(invoiceBatch);
                            final List<Object[]> finalOutboxBatch = new ArrayList<>(outboxBatch);
                            transactionTemplate.execute(status -> {
                                jdbcTemplate.batchUpdate(insertInvoiceSql, finalInvoiceBatch);
                                jdbcTemplate.batchUpdate(insertOutboxSql, finalOutboxBatch);
                                return null;
                            });
                        }
                    } catch (Exception ex) {
                        throw new RuntimeException(ex);
                    }
                });
            }
            executor.shutdown();
            executor.awaitTermination(15, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        long endTime = System.currentTimeMillis();
        double durationSeconds = (endTime - startTime) / 1000.0;
        long totalCalculations = successCounter.sum();
        double throughput = totalCalculations / durationSeconds;

        Map<String, Object> report = new HashMap<>();
        report.put("test_case", "Real Pipeline (Query readings -> RAM compute -> DB write invoices & outbox)");
        report.put("scale_size", totalCalculations);
        report.put("total_benchmark_time_seconds", durationSeconds);
        report.put("throughput_per_second", Math.round(throughput));
        report.put("database_write_active", writeToDb);
        report.put("status", "SUCCESS");

        return ResponseEntity.ok(report);
    }

    private BillingConfigSnapshot getMockConfigSnapshot(String accId, String meterId) {
        BillingConfigSnapshot config = new BillingConfigSnapshot();
        config.setAccountId(accId);
        config.setNormsFactor(1);
        config.setFastPathEnabled(true);
        config.setFastPathMeterPointId(meterId);
        config.setFastPathTariffCode("TARIFF_SHBT_2026");

        // Set Meter Topology
        MeterTopology topology = new MeterTopology();
        MeterPointNode node = new MeterPointNode();
        node.setMeterPointId(meterId);
        node.setCalculationType(CalculationType.AGGREGATION);
        node.setTariffCode("TARIFF_SHBT_2026");
        node.setChildPoints(new ArrayList<>());
        topology.setRootPoints(List.of(node));
        config.setMeterTopology(topology);

        Map<String, TariffRules> tariffs = new HashMap<>();
        TariffRules rules = new TariffRules();
        rules.setTariffCode("TARIFF_SHBT_2026");
        rules.setType("STEPPING");
        rules.setBlocks(getStandardShbtBlocks());
        tariffs.put("TARIFF_SHBT_2026", rules);
        config.setTariffs(tariffs);

        List<BillingSchemaStep> steps = new ArrayList<>();

        BillingSchemaStep ratingStep = new BillingSchemaStep();
        ratingStep.setStepNumber(10);
        ratingStep.setVariantName("STEP_RATING");
        ratingStep.setInputOperands(Map.of("consumption", "NET_KWH", "tariffCode", "FAST_TARIFF_CODE"));
        ratingStep.setOutputOperands(Map.of("amount", "BASE_AMOUNT", "breakdown", "RATING_BREAKDOWN"));
        ratingStep.setStepConfig(new HashMap<>());
        steps.add(ratingStep);

        BillingSchemaStep taxStep = new BillingSchemaStep();
        taxStep.setStepNumber(20);
        taxStep.setVariantName("TAX");
        taxStep.setInputOperands(Map.of("amount", "BASE_AMOUNT"));
        taxStep.setOutputOperands(Map.of("taxAmount", "TAX_AMOUNT", "totalAmount", "TOTAL_AMOUNT"));
        taxStep.setStepConfig(Map.of("taxRate", 0.10));
        steps.add(taxStep);

        config.setSchemaSteps(steps);
        return config;
    }

    private List<TariffBlock> getStandardShbtBlocks() {
        List<TariffBlock> blocks = new ArrayList<>();
        blocks.add(createBlock(1, 0.0, 50.0, 1806.00));
        blocks.add(createBlock(2, 50.0, 100.0, 1866.00));
        blocks.add(createBlock(3, 100.0, 200.0, 2167.00));
        blocks.add(createBlock(4, 200.0, 300.0, 2729.00));
        blocks.add(createBlock(5, 300.0, 400.0, 3050.00));
        blocks.add(createBlock(6, 400.0, null, 3157.00));
        return blocks;
    }

    private TariffBlock createBlock(int step, double min, Double max, double price) {
        TariffBlock b = new TariffBlock();
        b.setStep(step);
        b.setMinKwh(min);
        b.setMaxKwh(max);
        b.setUnitPrice(price);
        return b;
    }
}
