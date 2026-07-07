package com.evn.billing.mediation.controller;

import com.evn.billing.common.domain.Account;
import com.evn.billing.common.domain.BillInvoice;
import com.evn.billing.common.domain.BillingAccountSnapshot;
import com.evn.billing.common.domain.MeterUsage;
import com.evn.billing.mediation.dto.CmisReadingEvent;
import com.evn.billing.mediation.repository.AccountRepository;
import com.evn.billing.mediation.repository.BillInvoiceRepository;
import com.evn.billing.mediation.repository.BillingAccountSnapshotRepository;
import com.evn.billing.mediation.repository.MeterUsageRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.transaction.annotation.Transactional;
import java.nio.ByteBuffer;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RestController
@RequestMapping("/api/v1/monitoring")
@CrossOrigin(origins = "*")
public class MonitoringPortalController {

    private static final Logger log = LoggerFactory.getLogger(MonitoringPortalController.class);

    @Autowired
    private AccountRepository accountRepository;

    @Autowired
    private MeterUsageRepository meterUsageRepository;

    @Autowired
    private BillingAccountSnapshotRepository snapshotRepository;

    @Autowired
    private BillInvoiceRepository billInvoiceRepository;

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private org.springframework.jdbc.core.JdbcTemplate jdbcTemplate;

    @Autowired
    private org.springframework.data.redis.core.StringRedisTemplate redisTemplate;

    private final RestTemplate restTemplate = new RestTemplate();

    @GetMapping("/accounts")
    public List<Account> getAccounts() {
        return accountRepository.findTop100ByOrderByAccountIdAsc();
    }

    @GetMapping("/readings")
    public List<MeterUsage> getReadings() {
        return meterUsageRepository.findAll();
    }

    @GetMapping("/snapshots")
    public List<BillingAccountSnapshot> getSnapshots() {
        return snapshotRepository.findAll();
    }

    @GetMapping("/invoices")
    public List<BillInvoice> getInvoices() {
        return billInvoiceRepository.findAll();
    }

    @GetMapping("/logs")
    public ResponseEntity<?> getCalculationLogs(
            @RequestParam(required = false) String bookId,
            @RequestParam(required = false) String accountId,
            @RequestParam(required = false) String status,
            @RequestParam(defaultValue = "50") int limit) {
        
        StringBuilder query = new StringBuilder("SELECT log_id, book_id, account_id, billing_cycle_month, status, error_message, created_at FROM billing_calculation_log WHERE 1=1 ");
        java.util.List<Object> args = new java.util.ArrayList<>();
        
        if (bookId != null && !bookId.trim().isEmpty()) {
            query.append("AND book_id = ? ");
            args.add(bookId.trim());
        }
        if (accountId != null && !accountId.trim().isEmpty()) {
            query.append("AND account_id = ? ");
            args.add(accountId.trim());
        }
        if (status != null && !status.trim().isEmpty()) {
            query.append("AND status = ? ");
            args.add(status.trim());
        }
        
        query.append("ORDER BY created_at DESC LIMIT ?");
        args.add(limit);
        
        try {
            List<Map<String, Object>> logs = jdbcTemplate.queryForList(query.toString(), args.toArray());
            return ResponseEntity.ok(logs);
        } catch (Exception e) {
            return ResponseEntity.ok(java.util.Collections.emptyList());
        }
    }

    @GetMapping("/logs/detail/{logId}")
    public ResponseEntity<?> getCalculationLogDetail(@PathVariable String logId) {
        try {
            Map<String, Object> log = jdbcTemplate.queryForMap(
                    "SELECT log_id, book_id, account_id, billing_cycle_month, status, input_data, output_data, error_message, created_at FROM billing_calculation_log WHERE log_id = ?::uuid",
                    logId);
            return ResponseEntity.ok(log);
        } catch (Exception e) {
            return ResponseEntity.notFound().build();
        }
    }

    @GetMapping("/batch/executions")
    public ResponseEntity<?> getBatchExecutions() {
        try {
            String sql = "SELECT e.job_execution_id, i.job_name, e.status, e.start_time, e.end_time, e.exit_code, e.exit_message, " +
                    "(SELECT parameter_value FROM batch_job_execution_params WHERE job_execution_id = e.job_execution_id AND parameter_name = 'bookId') as book_id, " +
                    "(SELECT parameter_value FROM batch_job_execution_params WHERE job_execution_id = e.job_execution_id AND parameter_name = 'month') as month " +
                    "FROM batch_job_execution e " +
                    "JOIN batch_job_instance i ON e.job_instance_id = i.job_instance_id " +
                    "ORDER BY e.job_execution_id DESC LIMIT 50";
            List<Map<String, Object>> executions = jdbcTemplate.queryForList(sql);
            return ResponseEntity.ok(executions);
        } catch (Exception e) {
            return ResponseEntity.ok(java.util.Collections.emptyList());
        }
    }

    @GetMapping("/batch/runs")
    public ResponseEntity<?> getBookBillingRuns() {
        try {
            String sql = "SELECT book_id, billing_cycle_month, period, status, run_status, total_accounts, processed_accounts, success_accounts, failed_accounts, updated_at " +
                    "FROM book_billing_schedule ORDER BY updated_at DESC LIMIT 50";
            List<Map<String, Object>> runs = jdbcTemplate.queryForList(sql);
            return ResponseEntity.ok(runs);
        } catch (Exception e) {
            return ResponseEntity.ok(java.util.Collections.emptyList());
        }
    }

    @GetMapping("/batch/executions/{jobExecutionId}/steps")
    public ResponseEntity<?> getBatchStepExecutions(@PathVariable Long jobExecutionId) {
        try {
            String sql = "SELECT step_name, status, start_time, end_time, read_count, write_count, exit_code, " +
                    "EXTRACT(EPOCH FROM (COALESCE(end_time, NOW()) - start_time)) as duration_seconds " +
                    "FROM batch_step_execution " +
                    "WHERE job_execution_id = ? " +
                    "ORDER BY step_execution_id ASC";
            List<Map<String, Object>> steps = jdbcTemplate.queryForList(sql, jobExecutionId);
            return ResponseEntity.ok(steps);
        } catch (Exception e) {
            log.error("Failed to query batch step executions: {}", e.getMessage(), e);
            return ResponseEntity.status(500).body("Error: " + e.getMessage());
        }
    }

    @GetMapping("/batch/runs/{bookId}/{month}/steps")
    public ResponseEntity<?> getBookBillingRunSteps(@PathVariable String bookId, @PathVariable String month) {
        try {
            String findJobIdSql = "SELECT e.job_execution_id FROM batch_job_execution e " +
                    "JOIN batch_job_execution_params p1 ON e.job_execution_id = p1.job_execution_id AND p1.parameter_name = 'bookId' AND p1.parameter_value = ? " +
                    "JOIN batch_job_execution_params p2 ON e.job_execution_id = p2.job_execution_id AND p2.parameter_name = 'month' AND p2.parameter_value = ? " +
                    "ORDER BY e.job_execution_id DESC LIMIT 1";
            List<Long> ids = jdbcTemplate.queryForList(findJobIdSql, Long.class, bookId, month);
            if (ids.isEmpty()) {
                return ResponseEntity.ok(java.util.Collections.emptyList());
            }
            return getBatchStepExecutions(ids.getFirst());
        } catch (Exception e) {
            log.error("Failed to query latest job step executions for book run: {}", e.getMessage(), e);
            return ResponseEntity.status(500).body("Error: " + e.getMessage());
        }
    }


    @PostMapping("/batch/run")
    public ResponseEntity<String> runBatchJob(
            @RequestParam String bookId,
            @RequestParam String month,
            @RequestParam(defaultValue = "1") Long version) {
        try {
            String url = "http://localhost:8083/api/v1/batch/run?bookId=" + bookId + "&month=" + month + "&version=" + version;
            return restTemplate.postForEntity(url, null, String.class);
        } catch (Exception e) {
            return ResponseEntity.status(500).body("Failed to call batch-orchestrator: " + e.getMessage());
        }
    }

    @DeleteMapping("/billing/cancel")
    public ResponseEntity<String> cancelBilling(
            @RequestParam String accountId,
            @RequestParam String month) {
        try {
            String url = "http://localhost:8081/api/v1/billing/cancel?accountId=" + accountId + "&month=" + month;
            restTemplate.delete(url);
            return ResponseEntity.ok("Successfully requested cancellation of billing for " + accountId);
        } catch (Exception e) {
            log.error("Failed to cancel billing via proxy: {}", e.getMessage(), e);
            return ResponseEntity.status(500).body("Failed to call billing-worker cancel endpoint: " + e.getMessage());
        }
    }

    @GetMapping("/batch/validate")
    public ResponseEntity<String> validateBatchJob(
            @RequestParam String bookId,
            @RequestParam String month) {
        try {
            String url = "http://localhost:8083/api/v1/batch/validate?bookId=" + bookId + "&month=" + month;
            return restTemplate.getForEntity(url, String.class);
        } catch (Exception e) {
            return ResponseEntity.status(500).body("Failed to call batch-orchestrator validation: " + e.getMessage());
        }
    }

    @GetMapping("/detail/{accountId}/{month}")
    public ResponseEntity<?> getDetail(
            @PathVariable String accountId,
            @PathVariable String month,
            @RequestParam(defaultValue = "1") Integer period) {
        Map<String, Object> detail = new HashMap<>();

        Optional<Account> accountOpt = accountRepository.findById(accountId);
        if (accountOpt.isEmpty()) {
            return ResponseEntity.notFound().build();
        }
        detail.put("account", accountOpt.get());

        List<MeterUsage> usages = meterUsageRepository.findByAccountIdAndBillingCycleMonthAndPeriod(accountId, month, period);
        detail.put("readings", usages);

        String snapshotId = accountId + "_" + month + "_p" + period + "_v1";
        Optional<BillingAccountSnapshot> snapshotOpt = snapshotRepository.findById(snapshotId);
        detail.put("snapshot", snapshotOpt.orElse(null));

        Optional<BillInvoice> invoiceOpt = billInvoiceRepository.findByAccountIdAndBillingCycleMonthAndPeriod(accountId, month, period);
        detail.put("invoice", invoiceOpt.orElse(null));

        return ResponseEntity.ok(detail);
    }

    @PostMapping("/simulate/snapshot")
    public ResponseEntity<String> simulateSnapshot(@RequestParam String bookId, @RequestParam String month) {
        try {
            String url = "http://localhost:8082/api/v1/snapshots/generate?bookId=" + bookId + "&month=" + month;
            return restTemplate.postForEntity(url, null, String.class);
        } catch (Exception e) {
            return ResponseEntity.status(500).body("Failed to call snapshot-generator service: " + e.getMessage());
        }
    }

    @PostMapping("/simulate/calculate")
    public ResponseEntity<String> simulateCalculate(
            @RequestParam String accountId,
            @RequestParam String month,
            @RequestParam(defaultValue = "1") Integer period,
            @RequestParam String bookId) {
        try {
            String url = "http://localhost:8081/api/v1/billing/calculate-immediate?accountId=" + accountId + "&month=" + month + "&period=" + period + "&bookId=" + bookId;
            return restTemplate.postForEntity(url, null, String.class);
        } catch (Exception e) {
            return ResponseEntity.status(500).body("Failed to call billing-worker service: " + e.getMessage());
        }
    }

    @PostMapping("/simulate/auto-flow")
    public ResponseEntity<String> simulateAutoFlow(
            @RequestParam String accountId,
            @RequestParam String bookId,
            @RequestParam String month,
            @RequestParam(defaultValue = "1") Integer period,
            @RequestParam String meterPointId,
            @RequestParam double startIndex,
            @RequestParam double endIndex) {
        
        try {
            // Coerce meterPointId for multi-rate accounts
            String actualMeterId = meterPointId;
            if ("KH004".equals(accountId)) {
                actualMeterId = "METER-04-BT";
            } else if ("KH005".equals(accountId)) {
                actualMeterId = "METER-05-BT";
            }

            // Step 1: Chốt snapshot nếu chưa tồn tại
            String snapshotId = accountId + "_" + month + "_p" + period + "_v1";
            Optional<BillingAccountSnapshot> snapshotOpt = snapshotRepository.findById(snapshotId);
            if (snapshotOpt.isEmpty()) {
                String url = "http://localhost:8082/api/v1/snapshots/generate?bookId=" + bookId + "&month=" + month + "&period=" + period;
                restTemplate.postForEntity(url, null, String.class);
            }

            // Step 2: Lưu chỉ số thô vào CSDL
            Optional<MeterUsage> existingOpt = meterUsageRepository
                    .findByAccountIdAndMeterPointIdAndBillingCycleMonthAndPeriod(accountId, actualMeterId, month, period);
            MeterUsage usage = existingOpt.orElseGet(MeterUsage::new);
            
            if (usage.getUsageId() == null) {
                usage.setUsageId(Math.abs(new Random().nextLong()));
            }
            usage.setAccountId(accountId);
            usage.setMeterPointId(actualMeterId);
            usage.setBillingCycleMonth(month);
            usage.setPeriod(period);
            usage.setFromDate(LocalDateTime.now().minusDays(30));
            usage.setToDate(LocalDateTime.now());
            usage.setStartIndex(BigDecimal.valueOf(startIndex));
            usage.setEndIndex(BigDecimal.valueOf(endIndex));
            usage.setStatus("VALIDATED");
            meterUsageRepository.save(usage);

            // Đối với KH003, tự động thêm chỉ số phụ cho METER-03-PHU
            if ("KH003".equals(accountId)) {
                Optional<MeterUsage> childOpt = meterUsageRepository
                        .findByAccountIdAndMeterPointIdAndBillingCycleMonthAndPeriod(accountId, "METER-03-PHU", month, period);
                MeterUsage childUsage = childOpt.orElseGet(MeterUsage::new);
                
                if (childUsage.getUsageId() == null) {
                    childUsage.setUsageId(Math.abs(new Random().nextLong()));
                }
                childUsage.setAccountId(accountId);
                childUsage.setMeterPointId("METER-03-PHU");
                childUsage.setBillingCycleMonth(month);
                childUsage.setPeriod(period);
                childUsage.setFromDate(LocalDateTime.now().minusDays(30));
                childUsage.setToDate(LocalDateTime.now());
                childUsage.setStartIndex(BigDecimal.ZERO);
                childUsage.setEndIndex(BigDecimal.valueOf(100)); // 100 kWh
                childUsage.setStatus("VALIDATED");
                meterUsageRepository.save(childUsage);
            }

            // Đối với KH004 (SHBT 3 giá), tự động thêm chỉ số cho CD và TD
            if ("KH004".equals(accountId)) {
                // CD
                Optional<MeterUsage> cdOpt = meterUsageRepository
                        .findByAccountIdAndMeterPointIdAndBillingCycleMonthAndPeriod(accountId, "METER-04-CD", month, period);
                MeterUsage cdUsage = cdOpt.orElseGet(MeterUsage::new);
                if (cdUsage.getUsageId() == null) cdUsage.setUsageId(Math.abs(new Random().nextLong()));
                cdUsage.setAccountId(accountId);
                cdUsage.setMeterPointId("METER-04-CD");
                cdUsage.setBillingCycleMonth(month);
                cdUsage.setPeriod(period);
                cdUsage.setFromDate(LocalDateTime.now().minusDays(30));
                cdUsage.setToDate(LocalDateTime.now());
                cdUsage.setStartIndex(BigDecimal.valueOf(500));
                cdUsage.setEndIndex(BigDecimal.valueOf(550));
                cdUsage.setStatus("VALIDATED");
                meterUsageRepository.save(cdUsage);

                // TD
                Optional<MeterUsage> tdOpt = meterUsageRepository
                        .findByAccountIdAndMeterPointIdAndBillingCycleMonthAndPeriod(accountId, "METER-04-TD", month, period);
                MeterUsage tdUsage = tdOpt.orElseGet(MeterUsage::new);
                if (tdUsage.getUsageId() == null) tdUsage.setUsageId(Math.abs(new Random().nextLong()));
                tdUsage.setAccountId(accountId);
                tdUsage.setMeterPointId("METER-04-TD");
                tdUsage.setBillingCycleMonth(month);
                tdUsage.setPeriod(period);
                tdUsage.setFromDate(LocalDateTime.now().minusDays(30));
                tdUsage.setToDate(LocalDateTime.now());
                tdUsage.setStartIndex(BigDecimal.valueOf(300));
                tdUsage.setEndIndex(BigDecimal.valueOf(400));
                tdUsage.setStatus("VALIDATED");
                meterUsageRepository.save(tdUsage);
            }

            // Đối với KH005 (Sản xuất TOU 3 giá), tự động thêm chỉ số cho CD và TD
            if ("KH005".equals(accountId)) {
                // CD
                Optional<MeterUsage> cdOpt = meterUsageRepository
                        .findByAccountIdAndMeterPointIdAndBillingCycleMonthAndPeriod(accountId, "METER-05-CD", month, period);
                MeterUsage cdUsage = cdOpt.orElseGet(MeterUsage::new);
                if (cdUsage.getUsageId() == null) cdUsage.setUsageId(Math.abs(new Random().nextLong()));
                cdUsage.setAccountId(accountId);
                cdUsage.setMeterPointId("METER-05-CD");
                cdUsage.setBillingCycleMonth(month);
                cdUsage.setPeriod(period);
                cdUsage.setFromDate(LocalDateTime.now().minusDays(30));
                cdUsage.setToDate(LocalDateTime.now());
                cdUsage.setStartIndex(BigDecimal.valueOf(500));
                cdUsage.setEndIndex(BigDecimal.valueOf(700));
                cdUsage.setStatus("VALIDATED");
                meterUsageRepository.save(cdUsage);

                // TD
                Optional<MeterUsage> tdOpt = meterUsageRepository
                        .findByAccountIdAndMeterPointIdAndBillingCycleMonthAndPeriod(accountId, "METER-05-TD", month, period);
                MeterUsage tdUsage = tdOpt.orElseGet(MeterUsage::new);
                if (tdUsage.getUsageId() == null) tdUsage.setUsageId(Math.abs(new Random().nextLong()));
                tdUsage.setAccountId(accountId);
                tdUsage.setMeterPointId("METER-05-TD");
                tdUsage.setBillingCycleMonth(month);
                tdUsage.setPeriod(period);
                tdUsage.setFromDate(LocalDateTime.now().minusDays(30));
                tdUsage.setToDate(LocalDateTime.now());
                tdUsage.setStartIndex(BigDecimal.valueOf(300));
                tdUsage.setEndIndex(BigDecimal.valueOf(800));
                tdUsage.setStatus("VALIDATED");
                meterUsageRepository.save(tdUsage);
            }

            // Step 3: Gọi API tính cước khẩn cấp của billing-worker
            String calcUrl = "http://localhost:8081/api/v1/billing/calculate-immediate?accountId=" + accountId + "&month=" + month + "&period=" + period + "&bookId=" + bookId;
            restTemplate.postForEntity(calcUrl, null, String.class);

            return ResponseEntity.ok("Luồng xử lý tự động hoàn tất thành công cho tài khoản " + accountId);
        } catch (Exception e) {
            return ResponseEntity.status(500).body("Lỗi thực thi luồng tự động: " + e.getMessage());
        }
    }

    @PostMapping("/simulate/delete-invoice")
    @Transactional
    public ResponseEntity<String> deleteInvoice(
            @RequestParam String accountId,
            @RequestParam String month,
            @RequestParam(defaultValue = "1") Integer period) {
        try {
            // Call billing-worker cancel billing logic first to handle statuses, cache, and logs
            try {
                String workerCancelUrl = "http://localhost:8081/api/v1/billing/cancel?accountId=" + accountId + "&month=" + month + "&period=" + period;
                restTemplate.delete(workerCancelUrl);
            } catch (Exception e) {
                log.warn("Failed to call billing-worker cancel endpoint during simulate/delete-invoice: {}", e.getMessage());
            }

            // Delete invoice
            Optional<BillInvoice> invoiceOpt = billInvoiceRepository.findByAccountIdAndBillingCycleMonthAndPeriod(accountId, month, period);
            invoiceOpt.ifPresent(billInvoice -> billInvoiceRepository.delete(billInvoice));
            
            // Delete meter usages to allow re-ingestion
            List<MeterUsage> usages = meterUsageRepository.findByAccountIdAndBillingCycleMonthAndPeriodAndStatus(accountId, month, period, "VALIDATED");
            if (!usages.isEmpty()) {
                meterUsageRepository.deleteAll(usages);
            }
            List<MeterUsage> pendingUsages = meterUsageRepository.findByAccountIdAndBillingCycleMonthAndPeriodAndStatus(accountId, month, period, "PENDING_MANUAL");
            if (!pendingUsages.isEmpty()) {
                meterUsageRepository.deleteAll(pendingUsages);
            }

            // Also clear Redis cache for snapshot
            String cacheKey = "snapshot:" + accountId + ":" + month;
            redisTemplate.delete(cacheKey);

            return ResponseEntity.ok("Đã xóa hóa đơn, trạng thái chốt cước và chỉ số đo xa của khách hàng " + accountId + " kỳ " + month + " thành công!");
        } catch (Exception e) {
            return ResponseEntity.status(500).body("Lỗi khi xóa dữ liệu tính cước: " + e.getMessage());
        }
    }

    @GetMapping("/simulate/benchmark")
    public ResponseEntity<?> proxyBenchmark(
            @RequestParam(defaultValue = "10000000") int iterations,
            @RequestParam(defaultValue = "16") int threads) {
        try {
            String url = "http://localhost:8081/api/v1/benchmark/rating-engine?iterations=" + iterations + "&threads=" + threads;
            return restTemplate.getForEntity(url, Map.class);
        } catch (Exception e) {
            return ResponseEntity.status(500).body("Failed to call billing-worker service benchmark: " + e.getMessage());
        }
    }

    @GetMapping("/simulate/benchmark-pipeline")
    public ResponseEntity<?> proxyBenchmarkPipeline(
            @RequestParam(defaultValue = "100000") int size,
            @RequestParam(defaultValue = "16") int threads,
            @RequestParam(defaultValue = "true") boolean writeToDb,
            @RequestParam(defaultValue = "2026_06") String month) {
        try {
            String url = "http://localhost:8081/api/v1/benchmark/pipeline?size=" + size + "&threads=" + threads + "&writeToDb=" + writeToDb + "&month=" + month;
            return restTemplate.getForEntity(url, Map.class);
        } catch (Exception e) {
            return ResponseEntity.status(500).body("Failed to call billing-worker service pipeline benchmark: " + e.getMessage());
        }
    }

    @GetMapping("/simulate/benchmark-seed")
    public ResponseEntity<String> proxyBenchmarkSeed(
            @RequestParam(defaultValue = "100000") int size,
            @RequestParam(defaultValue = "2026_06") String month) {
        try {
            String url = "http://localhost:8081/api/v1/benchmark/seed?size=" + size + "&month=" + month;
            return restTemplate.getForEntity(url, String.class);
        } catch (Exception e) {
            return ResponseEntity.status(500).body("Failed to seed benchmark data: " + e.getMessage());
        }
    }

    @GetMapping("/simulate/benchmark-clean")
    public ResponseEntity<String> proxyBenchmarkClean(@RequestParam(required = false) String month) {
        try {
            String url = "http://localhost:8081/api/v1/benchmark/clean";
            if (month != null && !month.trim().isEmpty()) {
                url += "?month=" + month;
            }
            return restTemplate.getForEntity(url, String.class);
        } catch (Exception e) {
            return ResponseEntity.status(500).body("Failed to clean benchmark data: " + e.getMessage());
        }
    }

    @GetMapping("/simulate/benchmark-kafka")
    public ResponseEntity<String> benchmarkKafkaPush(
            @RequestParam(defaultValue = "100000") int size,
            @RequestParam(defaultValue = "16") int threads,
            @RequestParam(defaultValue = "2026_06") String month) {

        log.info("Cleaning previous Redis performance metrics...");
        try {
            redisTemplate.delete("benchmark:total_latency_e2e");
            redisTemplate.delete("benchmark:total_latency_ingest");
            redisTemplate.delete("benchmark:total_latency_queue");
            redisTemplate.delete("benchmark:total_latency_calc");
            redisTemplate.delete("benchmark:total_count");
            redisTemplate.delete("benchmark:latencies:e2e");
        } catch (Exception e) {
            log.error("Failed to clear Redis keys: {}", e.getMessage());
        }

        // Clean previous DB mock invoices and usages for the benchmark month to prevent DuplicateKeyException
        try {
            jdbcTemplate.execute("DELETE FROM bill_invoice WHERE account_id LIKE 'MOCK_KH_%' AND billing_cycle_month = '" + month + "'");
            jdbcTemplate.execute("DELETE FROM meter_usage WHERE account_id LIKE 'MOCK_KH_%' AND billing_cycle_month = '" + month + "'");
            jdbcTemplate.execute("DELETE FROM billing_calculation_log WHERE account_id LIKE 'MOCK_KH_%' AND billing_cycle_month = '" + month + "'");
            log.info("Cleaned old mock invoices, usages, and logs in DB for month {} before starting Kafka benchmark.", month);
        } catch (Exception e) {
            log.error("Failed to clean DB benchmark data for month {}: {}", month, e.getMessage());
        }

        log.info("Pushing {} CMIS events to Kafka...", size);
        long startTime = System.currentTimeMillis();

        int chunkSize = Math.max(1, size / threads);
        try (java.util.concurrent.ExecutorService executor = java.util.concurrent.Executors.newFixedThreadPool(threads)) {
            for (int t = 0; t < threads; t++) {
                final int threadId = t;
                executor.submit(() -> {
                    int startIdx = threadId * chunkSize;
                    int endIdx = Math.min(startIdx + chunkSize, size);
                    Random rand = new Random();

                    for (int i = startIdx; i < endIdx; i++) {
                        String accId = "MOCK_KH_" + i;
                        String meterId = "MOCK_METER_" + i;

                        CmisReadingEvent event = new CmisReadingEvent();
                        event.setAccountId(accId);
                        event.setMeterPointId(meterId);
                        event.setStartIndex(BigDecimal.valueOf(1000.0));
                        event.setEndIndex(BigDecimal.valueOf(1000.0 + 50.0 + rand.nextInt(350)));
                        event.setBillingCycleMonth(month);

                        long t1Ingest = System.currentTimeMillis();
                        ProducerRecord<String, Object> producerRecord = new ProducerRecord<>("meter-readings-input", accId, event);
                        producerRecord.headers().add("t1_ingest", ByteBuffer.allocate(8).putLong(t1Ingest).array());
                        kafkaTemplate.send(producerRecord);
                    }
                });
            }
            executor.shutdown();
            executor.awaitTermination(30, TimeUnit.MINUTES);
        } catch (Exception e) {
            return ResponseEntity.status(500).body("Error pushing to Kafka: " + e.getMessage());
        }

        long duration = System.currentTimeMillis() - startTime;
        double throughput = size / (duration / 1000.0);
        return ResponseEntity.ok("Đã đẩy thành công " + size + " sự kiện CMIS lên Kafka. Thời gian: " + (duration / 1000.0) + " giây. Thông lượng đẩy: " + Math.round(throughput) + " msg/s.");
    }

    @GetMapping("/simulate/benchmark-progress")
    public ResponseEntity<Map<String, Object>> getBenchmarkProgress() {
        Long count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM bill_invoice WHERE invoice_id LIKE 'MOCK_INV_%' OR invoice_id LIKE 'INV-MOCK_KH_%'", Long.class);
        Map<String, Object> progress = new HashMap<>();
        progress.put("processed_invoices", count != null ? count : 0L);
        progress.put("timestamp", System.currentTimeMillis());

        try {
            String totalE2e = redisTemplate.opsForValue().get("benchmark:total_latency_e2e");
            String totalIngest = redisTemplate.opsForValue().get("benchmark:total_latency_ingest");
            String totalQueue = redisTemplate.opsForValue().get("benchmark:total_latency_queue");
            String totalCalc = redisTemplate.opsForValue().get("benchmark:total_latency_calc");
            String totalCountStr = redisTemplate.opsForValue().get("benchmark:total_count");

            long totalCount = totalCountStr != null ? Long.parseLong(totalCountStr) : 0L;
            if (totalCount > 0) {
                progress.put("avg_e2e_ms", Double.parseDouble(totalE2e) / totalCount);
                progress.put("avg_ingest_ms", Double.parseDouble(totalIngest) / totalCount);
                progress.put("avg_queue_ms", Double.parseDouble(totalQueue) / totalCount);
                progress.put("avg_calc_ms", Double.parseDouble(totalCalc) / totalCount);
            } else {
                progress.put("avg_e2e_ms", 0.0);
                progress.put("avg_ingest_ms", 0.0);
                progress.put("avg_queue_ms", 0.0);
                progress.put("avg_calc_ms", 0.0);
            }

            List<String> rawLatencies = redisTemplate.opsForList().range("benchmark:latencies:e2e", 0, -1);
            if (rawLatencies != null && !rawLatencies.isEmpty()) {
                List<Double> latencies = new java.util.ArrayList<>();
                for (String s : rawLatencies) {
                    latencies.add(Double.parseDouble(s));
                }
                java.util.Collections.sort(latencies);
                int len = latencies.size();
                int idx95 = (int) Math.ceil(len * 0.95) - 1;
                int idx99 = (int) Math.ceil(len * 0.99) - 1;
                progress.put("p95_e2e_ms", latencies.get(Math.max(0, idx95)));
                progress.put("p99_e2e_ms", latencies.get(Math.max(0, idx99)));
            } else {
                progress.put("p95_e2e_ms", 0.0);
                progress.put("p99_e2e_ms", 0.0);
            }
        } catch (Exception e) {
            progress.put("avg_e2e_ms", 0.0);
            progress.put("avg_ingest_ms", 0.0);
            progress.put("avg_queue_ms", 0.0);
            progress.put("avg_calc_ms", 0.0);
            progress.put("p95_e2e_ms", 0.0);
            progress.put("p99_e2e_ms", 0.0);
        }

        return ResponseEntity.ok(progress);
    }
}
