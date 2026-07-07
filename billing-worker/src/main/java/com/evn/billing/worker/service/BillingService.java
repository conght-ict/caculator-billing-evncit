package com.evn.billing.worker.service;

import com.evn.billing.common.domain.BillInvoice;
import com.evn.billing.common.domain.BillingAccountSnapshot;
import com.evn.billing.common.domain.MeterUsage;
import com.evn.billing.common.domain.OutboxEvent;
import com.evn.billing.common.domain.AccountBillingStatus;
import com.evn.billing.common.domain.AccountBillingStatusId;
import com.evn.billing.common.domain.BookBillingSchedule;
import com.evn.billing.common.dto.BillingConfigSnapshot;
import com.evn.billing.common.dto.MeterPointNode;
import com.evn.billing.engine.BillingCalculator;
import com.evn.billing.engine.CalculationResult;
import com.evn.billing.worker.dto.BillingTaskDto;
import com.evn.billing.worker.repository.BillInvoiceRepository;
import com.evn.billing.worker.repository.BillingAccountSnapshotRepository;
import com.evn.billing.worker.repository.MeterUsageRepository;
import com.evn.billing.worker.repository.OutboxEventRepository;
import com.evn.billing.worker.repository.AccountBillingStatusRepository;
import com.evn.billing.worker.repository.BookBillingScheduleRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;

@Service
public class BillingService {

    private static final Logger log = LoggerFactory.getLogger(BillingService.class);

    @Autowired
    private MeterUsageRepository meterUsageRepository;

    @Autowired
    private BillingAccountSnapshotRepository snapshotRepository;

    @Autowired
    private BillInvoiceRepository billInvoiceRepository;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private AccountBillingStatusRepository accountBillingStatusRepository;

    @Autowired
    private BookBillingScheduleRepository bookBillingScheduleRepository;

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private BillingLogService billingLogService;

    @Autowired
    private org.springframework.jdbc.core.JdbcTemplate jdbcTemplate;

    private final BillingCalculator billingCalculator = new BillingCalculator();

    private final Map<String, Map<String, String>> localBookStatusCache = new java.util.concurrent.ConcurrentHashMap<>();

    public void warmupBookCache(String bookId, String month, int period) {
        if (bookId == null || bookId.isEmpty()) return;
        String cacheWarmedKey = "billing:book_warmed:" + bookId + ":" + month + ":" + period;
        String hashKey = "billing:book_status_hash:" + bookId + ":" + month + ":" + period;
        String localKey = bookId + ":" + month + ":" + period;

        if (localBookStatusCache.containsKey(localKey)) {
            return;
        }

        Boolean hasRedisKey = false;
        try {
            hasRedisKey = redisTemplate.hasKey(cacheWarmedKey);
        } catch (Exception e) {
            log.warn("[WARMUP] Redis is offline. Fallback to Postgres status check directly.");
        }

        if (hasRedisKey == null || !hasRedisKey) {
            log.info("[WARMUP] Cache miss for Book: {}, Month: {}, Period: {}. Loading from Postgres...", bookId, month, period);
            List<AccountBillingStatus> dbStatuses = accountBillingStatusRepository.findByBookIdAndBillingCycleMonthAndPeriod(bookId, month, period);
            Map<String, String> statusMap = new HashMap<>();
            for (AccountBillingStatus abs : dbStatuses) {
                statusMap.put(abs.getAccountId(), abs.getStatus());
            }

            try {
                if (!statusMap.isEmpty()) {
                    redisTemplate.opsForHash().putAll(hashKey, statusMap);
                }
                redisTemplate.opsForValue().set(cacheWarmedKey, "true", 24, TimeUnit.HOURS);
                redisTemplate.expire(hashKey, 30, TimeUnit.DAYS);
            } catch (Exception e) {
                log.warn("[WARMUP] Failed to write warmed status back to Redis: {}", e.getMessage());
            }
            log.info("[WARMUP] Cache warmed for Book: {}, Month: {}, Period: {}. Loaded {} statuses.", bookId, month, period, statusMap.size());
        }

        Map<String, String> localMap = new java.util.concurrent.ConcurrentHashMap<>();
        try {
            Map<Object, Object> redisHash = redisTemplate.opsForHash().entries(hashKey);
            for (Map.Entry<Object, Object> entry : redisHash.entrySet()) {
                localMap.put(entry.getKey().toString(), entry.getValue().toString());
            }
        } catch (Exception e) {
            log.warn("[WARMUP] Redis lookup failed, loading locally from Postgres for thread safety.");
            List<AccountBillingStatus> dbStatuses = accountBillingStatusRepository.findByBookIdAndBillingCycleMonthAndPeriod(bookId, month, period);
            for (AccountBillingStatus abs : dbStatuses) {
                localMap.put(abs.getAccountId(), abs.getStatus());
            }
        }
        localBookStatusCache.put(localKey, localMap);
        log.info("[WARMUP] JVM Memory loaded for Book: {}, Month: {}, Period: {}. Total in-memory keys: {}", bookId, month, period, localMap.size());
    }

    public String getAccountStatus(String bookId, String accountId, String month, int period) {
        if (bookId == null || bookId.isEmpty()) return null;
        String localKey = bookId + ":" + month + ":" + period;
        Map<String, String> localMap = localBookStatusCache.get(localKey);
        if (localMap != null && localMap.containsKey(accountId)) {
            return localMap.get(accountId);
        }

        String hashKey = "billing:book_status_hash:" + bookId + ":" + month + ":" + period;
        try {
            Object val = redisTemplate.opsForHash().get(hashKey, accountId);
            if (val != null) {
                return val.toString();
            }
        } catch (Exception e) {
            // Ignore
        }

        Optional<AccountBillingStatus> dbStatus = accountBillingStatusRepository
                .findById(new AccountBillingStatusId(accountId, month, period));
        if (dbStatus.isPresent()) {
            String dbVal = dbStatus.get().getStatus();
            try {
                redisTemplate.opsForHash().put(hashKey, accountId, dbVal);
            } catch (Exception e) {
                // Ignore
            }
            return dbVal;
        }

        return null;
    }

    public void updateAccountStatus(String bookId, String accountId, String month, int period, String status, String invoiceId, String errorMsg, Long durationMs, String workerNode) {
        if (bookId == null || bookId.isEmpty()) return;
        AccountBillingStatus abs = new AccountBillingStatus();
        abs.setAccountId(accountId);
        abs.setBillingCycleMonth(month);
        abs.setPeriod(period);
        abs.setBookId(bookId);
        abs.setStatus(status);
        abs.setInvoiceId(invoiceId);
        abs.setErrorMessage(errorMsg);
        abs.setProcessingTimeMs(durationMs);
        abs.setWorkerNode(workerNode);
        abs.setUpdatedAt(LocalDateTime.now());
        accountBillingStatusRepository.save(abs);

        String hashKey = "billing:book_status_hash:" + bookId + ":" + month + ":" + period;
        try {
            redisTemplate.opsForHash().put(hashKey, accountId, status);
        } catch (Exception e) {
            // Ignore
        }

        String localKey = bookId + ":" + month + ":" + period;
        Map<String, String> localMap = localBookStatusCache.get(localKey);
        if (localMap != null) {
            localMap.put(accountId, status);
        }
    }

    public void updateBookBillingRunProgress(String bookId, String month, int period, int processedDelta, int successDelta, int failedDelta) {
        if (bookId == null || bookId.isEmpty()) return;
        String sql = "UPDATE book_billing_schedule SET " +
                "processed_accounts = processed_accounts + ?, " +
                "success_accounts = success_accounts + ?, " +
                "failed_accounts = failed_accounts + ?, " +
                "updated_at = NOW() " +
                "WHERE book_id = ? AND billing_cycle_month = ? AND period = ?";
        jdbcTemplate.update(sql, processedDelta, successDelta, failedDelta, bookId, month, period);
    }

    /**
     * Executes the rating calculations for a specific customer account in the batch cycle,
     * saving the resulting Invoice and the Outbox Event atomically.
     * 
     * @param task The task payload consumed from Kafka containing account metadata.
     */
    @Transactional
    public void processBilling(BillingTaskDto task) throws Exception {
        long tStart = System.currentTimeMillis();
        String accountId = task.getAccountId();
        String month = task.getBillingCycleMonth();
        int version = task.getCalculationVersion();
        String bookId = task.getBookId() != null ? task.getBookId() : "DEMAND";

        // 0. Check calculation status (Skip if already SUCCESS)
        warmupBookCache(bookId, month, task.getPeriod());
        String currentStatus = getAccountStatus(bookId, accountId, month, task.getPeriod());
        if ("SUCCESS".equals(currentStatus)) {
            log.info("[SKIP-CALC] Account {} is already calculated successfully for month {}. Aborting on-demand calculation.", accountId, month);
            throw new IllegalStateException("Khách hàng đã được tính cước thành công. Vui lòng hủy hóa đơn cũ trước khi tính lại.");
        }

        try {
            // 1. Get validated usages from Kafka payload to avoid DB read
            List<MeterUsage> usages = new ArrayList<>();
            if (task.getReadings() != null && !task.getReadings().isEmpty()) {
                for (com.evn.billing.worker.dto.MeterReadingDto r : task.getReadings()) {
                    MeterUsage u = new MeterUsage();
                    u.setMeterPointId(r.getMeterPointId());
                    u.setFromDate(r.getFromDate());
                    u.setToDate(r.getToDate());
                    u.setStartIndex(r.getStartIndex());
                    u.setEndIndex(r.getEndIndex());
                    u.setConsumption(r.getConsumption());
                    u.setAccountId(accountId);
                    u.setBillingCycleMonth(month);
                    u.setStatus("VALIDATED");
                    u.setIsRollover(r.getIsRollover() != null ? r.getIsRollover() : false);
                    u.setMaxRegisterSnapshot(r.getMaxRegisterSnapshot());
                    u.setSubReadingSeq(r.getSubReadingSeq() != null ? r.getSubReadingSeq() : 1);
                    u.setRecordType(r.getRecordType() != null ? r.getRecordType() : "ORIGINAL");
                    usages.add(u);
                }
            } else {
                // Fallback to database query if no readings provided in Kafka payload (e.g. REST fallback)
                usages = meterUsageRepository.findByAccountIdAndBillingCycleMonthAndPeriodAndStatus(accountId, month, task.getPeriod(), "VALIDATED");
            }
            if (usages.isEmpty()) {
                throw new NoSuchElementException("No validated meter usage found/provided for account: " + accountId);
            }

            // Determine actual billing period length (daysUsed)
            LocalDateTime minFrom = null;
            LocalDateTime maxTo = null;
            for (MeterUsage u : usages) {
                if (u.getFromDate() != null) {
                    if (minFrom == null || u.getFromDate().isBefore(minFrom)) {
                        minFrom = u.getFromDate();
                    }
                }
                if (u.getToDate() != null) {
                    if (maxTo == null || u.getToDate().isAfter(maxTo)) {
                        maxTo = u.getToDate();
                    }
                }
            }
            if (minFrom == null) minFrom = LocalDateTime.now().minusDays(30);
            if (maxTo == null) maxTo = LocalDateTime.now();
            long daysUsed = java.time.temporal.ChronoUnit.DAYS.between(minFrom.toLocalDate(), maxTo.toLocalDate()) + 1;

            // Compute actual days of that billing cycle month
            int daysInMonth = 30;
            if (month != null && month.contains("_")) {
                try {
                    String[] parts = month.split("_");
                    int year = Integer.parseInt(parts[0]);
                    int monthVal = Integer.parseInt(parts[1]);
                    java.time.YearMonth yearMonth = java.time.YearMonth.of(year, monthVal);
                    daysInMonth = yearMonth.lengthOfMonth();
                } catch (Exception e) {
                    // Ignore
                }
            }

            // 2. Fetch Frozen Snapshot from Redis Cache (Cache-aside)
            String cacheKey = "snapshot:" + accountId + ":" + month;
            BillingConfigSnapshot config = null;
            try {
                Object cachedObj = redisTemplate.opsForValue().get(cacheKey);
                if (cachedObj != null) {
                    if (cachedObj instanceof BillingConfigSnapshot) {
                        config = (BillingConfigSnapshot) cachedObj;
                    } else {
                        config = objectMapper.convertValue(cachedObj, BillingConfigSnapshot.class);
                    }
                }
            } catch (Exception e) {
                log.warn("Redis cluster cache offline, falling back to database: {}", e.getMessage());
            }

            if (config == null) {
                Optional<BillingAccountSnapshot> snapshotOpt = snapshotRepository
                        .findByAccountIdAndBillingCycleMonthAndPeriodAndCalculationVersion(accountId, month, task.getPeriod(), version);
                if (snapshotOpt.isEmpty()) {
                    throw new NoSuchElementException("No snapshot profile found for account: " + accountId + ", version: " + version);
                }
                config = snapshotOpt.get().getConfigData();
                try {
                    redisTemplate.opsForValue().set(cacheKey, config, 24, TimeUnit.HOURS);
                } catch (Exception e) {
                    // Ignore
                }
            }

            // [II.2] Validate Snapshot completeness (Self-Containment validation check)
            validateSnapshot(config, accountId);

            log.info("[AUDIT-TRACER] [Account: {}] Step 4: Kafka calculation task received. Triggering billing engine processing.", accountId);

            // 3. Collect node consumptions
            Map<String, BigDecimal> consumptions = new HashMap<>();
            for (MeterUsage u : usages) {
                BigDecimal cons = u.getConsumption() != null ? u.getConsumption() 
                        : u.getEndIndex().subtract(u.getStartIndex());
                consumptions.put(u.getMeterPointId(), cons);
            }

            BigDecimal proRataFactor = BigDecimal.ONE;
            if (daysUsed < daysInMonth && daysUsed > 0) {
                proRataFactor = BigDecimal.valueOf(daysUsed).divide(BigDecimal.valueOf(daysInMonth), 8, RoundingMode.HALF_UP);
            }

            log.info("[AUDIT-TRACER] [Account: {}] Step 5: Billing engine started. Tariffs={}. Days in month: {} days. Pro-rata days used: {} days (Pro-rata factor: {}).", 
                    accountId, config.getTariffs().keySet(), daysInMonth, daysUsed, proRataFactor);

            // 4. Invoke Core Stateless Rating Engine
            CalculationResult result = billingCalculator.calculate(config, consumptions, month, daysUsed);

            BigDecimal totalBeforeTax = result.getTotalAmountBeforeTax();
            BigDecimal taxAmount = result.getTaxAmount();
            BigDecimal totalAfterTax = result.getTotalAmountAfterTax();
            Map<String, Object> meterPointBreakdowns = result.getMeterPointBreakdowns();
            List<Map<String, Object>> stepDetails = result.getStepDetails();
            Map<String, BigDecimal> nodeNetConsumptions = result.getNodeNetConsumptions();

            log.info("[AUDIT-TRACER] [Account: {}] Step 5.1: Billing engine finished. Total Net Consumption: {}, Total Amount before tax: {}, VAT Tax Amount: {}, Total Amount after tax: {}.", 
                    accountId, nodeNetConsumptions, totalBeforeTax, taxAmount, totalAfterTax);

            // 5. Construct self-explanatory billing_manifest JSONB [IV.2]
            Map<String, Object> manifest = new HashMap<>();
            manifest.put("invoice_id", "INV-" + accountId + "-" + month + "-v" + version);
            manifest.put("calculation_engine_version", "v2.1-stable");
            manifest.put("timestamp", LocalDateTime.now().toString());
            manifest.put("snapshot_applied", accountId + "_" + month + "_v" + version);

            Map<String, Object> topologyCalculation = new HashMap<>();
            List<Map<String, Object>> inputReadings = new ArrayList<>();
            for (MeterUsage u : usages) {
                Map<String, Object> ir = new HashMap<>();
                ir.put("meter_point_id", u.getMeterPointId());
                ir.put("calculation_type", getCalculationType(config, u.getMeterPointId()));
                
                List<Map<String, Object>> subReadings = new ArrayList<>();
                Map<String, Object> sub = new HashMap<>();
                sub.put("seq", u.getSubReadingSeq());
                sub.put("from_date", u.getFromDate() != null ? u.getFromDate().toString() : null);
                sub.put("to_date", u.getToDate() != null ? u.getToDate().toString() : null);
                sub.put("start_index", u.getStartIndex());
                sub.put("end_index", u.getEndIndex());
                sub.put("is_rollover", u.getIsRollover());
                sub.put("max_register_value", u.getMaxRegisterSnapshot());
                sub.put("raw_consumption", u.getConsumption());
                subReadings.add(sub);
                
                ir.put("sub_readings", subReadings);
                ir.put("total_kwh", u.getConsumption());
                inputReadings.add(ir);
            }
            topologyCalculation.put("input_readings", inputReadings);
            topologyCalculation.put("node_net_consumptions", nodeNetConsumptions);
            manifest.put("topology_calculation", topologyCalculation);

            Map<String, Object> breakdown = new HashMap<>();
            breakdown.put("norms_factor", config.getNormsFactor());
            
            List<Map<String, Object>> stepsExecuted = new ArrayList<>();
            for (Map<String, Object> sd : stepDetails) {
                Map<String, Object> se = new HashMap<>();
                se.put("meter_point_id", sd.get("meter_point_id"));
                se.put("step", sd.get("step"));
                se.put("kwh_consumed", sd.get("kwh"));
                se.put("unit_price", sd.get("price"));
                se.put("amount", sd.get("amount"));
                stepsExecuted.add(se);
            }
            breakdown.put("steps_executed", stepsExecuted);
            breakdown.put("total_before_tax", totalBeforeTax);
            manifest.put("rating_breakdown", breakdown);

            Map<String, Object> taxCalc = new HashMap<>();
            taxCalc.put("vat_rate", 0.10);
            taxCalc.put("tax_amount_raw", taxAmount);
            taxCalc.put("rounding_mode", "HALF_UP");
            taxCalc.put("tax_amount_final", taxAmount);
            manifest.put("tax_calculation", taxCalc);
            
            manifest.put("total_final_amount", totalAfterTax);

            String manifestJson = objectMapper.writeValueAsString(manifest);
            String invoiceId = "INV-" + accountId + "-" + month + "-v" + version;
            String idempotencyKey = accountId + "_" + month + "_v" + version;

            // [IV.1] True atomic DB-level UPSERT on bill_invoice (ON CONFLICT DO UPDATE)
            String insertInvoiceSql = "INSERT INTO bill_invoice (" +
                    "invoice_id, account_id, book_id, billing_cycle_month, " +
                    "total_amount_before_tax, tax_amount, total_amount_after_tax, " +
                    "idempotency_key, billing_manifest, proration_applied, " +
                    "snapshot_ref, calculation_status, created_at, updated_at) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?, ?, ?, ?, ?) " +
                    "ON CONFLICT (idempotency_key) " +
                    "DO UPDATE SET " +
                    "total_amount_before_tax = EXCLUDED.total_amount_before_tax, " +
                    "tax_amount              = EXCLUDED.tax_amount, " +
                    "total_amount_after_tax  = EXCLUDED.total_amount_after_tax, " +
                    "billing_manifest        = EXCLUDED.billing_manifest, " +
                    "proration_applied       = EXCLUDED.proration_applied, " +
                    "snapshot_ref            = EXCLUDED.snapshot_ref, " +
                    "calculation_status      = EXCLUDED.calculation_status, " +
                    "updated_at              = NOW()";

            boolean isProrated = proRataFactor.compareTo(BigDecimal.ONE) < 0;

            jdbcTemplate.update(insertInvoiceSql,
                invoiceId, accountId, bookId, month,
                totalBeforeTax, taxAmount, totalAfterTax,
                idempotencyKey, manifestJson, isProrated,
                accountId + "_" + month + "_v" + version, "FINAL",
                java.sql.Timestamp.valueOf(LocalDateTime.now()), java.sql.Timestamp.valueOf(LocalDateTime.now())
            );

            log.info("[AUDIT-TRACER] [Account: {}] Step 6: UPSERT transaction completed. Invoice saved to 'bill_invoice'.", accountId);

            // 7. Save Outbox Event (Transactional Outbox Pattern)
            Map<String, Object> outboxPayload = new HashMap<>();
            outboxPayload.put("invoiceId", invoiceId);
            outboxPayload.put("accountId", accountId);
            outboxPayload.put("billingCycleMonth", month);
            outboxPayload.put("amountBeforeTax", totalBeforeTax);
            outboxPayload.put("taxAmount", taxAmount);
            outboxPayload.put("amountAfterTax", totalAfterTax);
            outboxPayload.put("timestamp", LocalDateTime.now().toString());

            String insertOutboxSql = "INSERT INTO outbox_event (event_id, aggregate_type, aggregate_id, event_type, payload, status, created_at) " +
                    "VALUES (?, ?, ?, ?, ?::jsonb, 'PENDING', ?)";

            jdbcTemplate.update(insertOutboxSql,
                UUID.randomUUID(), "INVOICE", invoiceId, "INVOICE_CREATED",
                objectMapper.writeValueAsString(outboxPayload), java.sql.Timestamp.valueOf(LocalDateTime.now())
            );

            log.info("[AUDIT-TRACER] [Account: {}] Step 6.1: Outbox event 'INVOICE_CREATED' saved.", accountId);

            // Update calculations tracking statuses
            long duration = System.currentTimeMillis() - tStart;
            updateAccountStatus(bookId, accountId, month, task.getPeriod(), "SUCCESS", invoiceId, null, duration, "WorkerNode1");
            updateBookBillingRunProgress(bookId, month, task.getPeriod(), 1, 1, 0);

            // Enqueue log
            String inputJson = objectMapper.writeValueAsString(consumptions);
            billingLogService.enqueueLog(bookId, accountId, month, task.getPeriod(), "SUCCESS", inputJson, manifestJson, null);
        } catch (Exception e) {
            long duration = System.currentTimeMillis() - tStart;
            updateAccountStatus(bookId, accountId, month, task.getPeriod(), "FAILED", null, e.getMessage(), duration, "WorkerNode1");
            updateBookBillingRunProgress(bookId, month, task.getPeriod(), 1, 0, 1);
            billingLogService.enqueueLog(bookId, accountId, month, task.getPeriod(), "FAILED", null, null, e.getMessage());
            throw e;
        }
    }

    @Transactional
    public void processBillingBatch(List<BillingTaskDto> tasks) throws Exception {
        if (tasks == null || tasks.isEmpty()) return;

        String firstBookId = tasks.get(0).getBookId() != null ? tasks.get(0).getBookId() : "DEMAND";
        String firstMonth = tasks.get(0).getBillingCycleMonth();
        
        warmupBookCache(firstBookId, firstMonth, tasks.get(0).getPeriod());

        List<Object[]> invoiceBatch = new ArrayList<>();
        List<Object[]> outboxBatch = new ArrayList<>();
        List<Object[]> statusBatch = new ArrayList<>();

        for (BillingTaskDto task : tasks) {
            String accountId = task.getAccountId();
            String month = task.getBillingCycleMonth();
            int version = task.getCalculationVersion();
            String bookId = task.getBookId() != null ? task.getBookId() : "DEMAND";

            String currentStatus = getAccountStatus(bookId, accountId, month, task.getPeriod());
            if ("SUCCESS".equals(currentStatus)) {
                log.info("[SKIP-CALC-BATCH] Account {} is already calculated successfully for month {}. Skipping in batch...", accountId, month);
                continue;
            }

            try {
                // 1. Get validated usages from Kafka task DTO readings
                List<MeterUsage> usages = new ArrayList<>();
                if (task.getReadings() != null && !task.getReadings().isEmpty()) {
                    for (com.evn.billing.worker.dto.MeterReadingDto r : task.getReadings()) {
                        MeterUsage u = new MeterUsage();
                        u.setMeterPointId(r.getMeterPointId());
                        u.setFromDate(r.getFromDate());
                        u.setToDate(r.getToDate());
                        u.setStartIndex(r.getStartIndex());
                        u.setEndIndex(r.getEndIndex());
                        u.setConsumption(r.getConsumption());
                        u.setAccountId(accountId);
                        u.setBillingCycleMonth(month);
                        u.setStatus("VALIDATED");
                        u.setIsRollover(r.getIsRollover() != null ? r.getIsRollover() : false);
                        u.setMaxRegisterSnapshot(r.getMaxRegisterSnapshot());
                        u.setSubReadingSeq(r.getSubReadingSeq() != null ? r.getSubReadingSeq() : 1);
                        u.setRecordType(r.getRecordType() != null ? r.getRecordType() : "ORIGINAL");
                        usages.add(u);
                    }
                } else {
                    usages = meterUsageRepository.findByAccountIdAndBillingCycleMonthAndPeriodAndStatus(accountId, month, task.getPeriod(), "VALIDATED");
                }
                if (usages.isEmpty()) {
                    throw new NoSuchElementException("No validated meter usage found/provided for account: " + accountId);
                }

                // Determine actual billing period length (daysUsed)
                LocalDateTime minFrom = null;
                LocalDateTime maxTo = null;
                for (MeterUsage u : usages) {
                    if (u.getFromDate() != null) {
                        if (minFrom == null || u.getFromDate().isBefore(minFrom)) {
                            minFrom = u.getFromDate();
                        }
                    }
                    if (u.getToDate() != null) {
                        if (maxTo == null || u.getToDate().isAfter(maxTo)) {
                            maxTo = u.getToDate();
                        }
                    }
                }
                if (minFrom == null) minFrom = LocalDateTime.now().minusDays(30);
                if (maxTo == null) maxTo = LocalDateTime.now();
                long daysUsed = java.time.temporal.ChronoUnit.DAYS.between(minFrom.toLocalDate(), maxTo.toLocalDate()) + 1;

                // Compute actual days of that billing cycle month
                int daysInMonth = 30;
                if (month != null && month.contains("_")) {
                    try {
                        String[] parts = month.split("_");
                        int year = Integer.parseInt(parts[0]);
                        int monthVal = Integer.parseInt(parts[1]);
                        java.time.YearMonth yearMonth = java.time.YearMonth.of(year, monthVal);
                        daysInMonth = yearMonth.lengthOfMonth();
                    } catch (Exception e) {
                        // Ignore
                    }
                }

                // 2. Fetch Frozen Snapshot from Redis Cache
                String cacheKey = "snapshot:" + accountId + ":" + month;
                BillingConfigSnapshot config = null;
                try {
                    Object cachedObj = redisTemplate.opsForValue().get(cacheKey);
                    if (cachedObj != null) {
                        if (cachedObj instanceof BillingConfigSnapshot) {
                            config = (BillingConfigSnapshot) cachedObj;
                        } else {
                            config = objectMapper.convertValue(cachedObj, BillingConfigSnapshot.class);
                        }
                    }
                } catch (Exception e) {
                    // Redis offline fallback
                }

                if (config == null) {
                    Optional<BillingAccountSnapshot> snapshotOpt = snapshotRepository
                            .findByAccountIdAndBillingCycleMonthAndPeriodAndCalculationVersion(accountId, month, task.getPeriod(), version);
                    if (snapshotOpt.isEmpty()) {
                        throw new NoSuchElementException("No snapshot profile found for account: " + accountId + ", version: " + version);
                    }
                    config = snapshotOpt.get().getConfigData();
                    try {
                        redisTemplate.opsForValue().set(cacheKey, config, 24, TimeUnit.HOURS);
                    } catch (Exception e) {
                        // Ignore
                    }
                }

                // [II.2] Validate Snapshot completeness
                validateSnapshot(config, accountId);

                // 3. Collect node consumptions
                Map<String, BigDecimal> consumptions = new HashMap<>();
                for (MeterUsage u : usages) {
                    BigDecimal cons = u.getConsumption() != null ? u.getConsumption() 
                            : u.getEndIndex().subtract(u.getStartIndex());
                    consumptions.put(u.getMeterPointId(), cons);
                }

                // 4. Invoke Core Stateless Rating Engine
                CalculationResult result = billingCalculator.calculate(config, consumptions, month, daysUsed);

                BigDecimal totalBeforeTax = result.getTotalAmountBeforeTax();
                BigDecimal taxAmount = result.getTaxAmount();
                BigDecimal totalAfterTax = result.getTotalAmountAfterTax();
                Map<String, Object> meterPointBreakdowns = result.getMeterPointBreakdowns();
                List<Map<String, Object>> stepDetails = result.getStepDetails();
                Map<String, BigDecimal> nodeNetConsumptions = result.getNodeNetConsumptions();

                // 5. Construct self-explanatory billing_manifest JSONB [IV.2]
                Map<String, Object> manifest = new HashMap<>();
                String invoiceId = "INV-" + accountId + "-" + month + "-v" + version;
                manifest.put("invoice_id", invoiceId);
                manifest.put("calculation_engine_version", "v2.1-stable");
                manifest.put("timestamp", LocalDateTime.now().toString());
                manifest.put("snapshot_applied", accountId + "_" + month + "_v" + version);

                Map<String, Object> topologyCalculation = new HashMap<>();
                List<Map<String, Object>> inputReadings = new ArrayList<>();
                for (MeterUsage u : usages) {
                    Map<String, Object> ir = new HashMap<>();
                    ir.put("meter_point_id", u.getMeterPointId());
                    ir.put("calculation_type", getCalculationType(config, u.getMeterPointId()));
                    
                    List<Map<String, Object>> subReadings = new ArrayList<>();
                    Map<String, Object> sub = new HashMap<>();
                    sub.put("seq", u.getSubReadingSeq());
                    sub.put("from_date", u.getFromDate() != null ? u.getFromDate().toString() : null);
                    sub.put("to_date", u.getToDate() != null ? u.getToDate().toString() : null);
                    sub.put("start_index", u.getStartIndex());
                    sub.put("end_index", u.getEndIndex());
                    sub.put("is_rollover", u.getIsRollover());
                    sub.put("max_register_value", u.getMaxRegisterSnapshot());
                    sub.put("raw_consumption", u.getConsumption());
                    subReadings.add(sub);
                    
                    ir.put("sub_readings", subReadings);
                    ir.put("total_kwh", u.getConsumption());
                    inputReadings.add(ir);
                }
                topologyCalculation.put("input_readings", inputReadings);
                topologyCalculation.put("node_net_consumptions", nodeNetConsumptions);
                manifest.put("topology_calculation", topologyCalculation);

                Map<String, Object> breakdown = new HashMap<>();
                breakdown.put("norms_factor", config.getNormsFactor());
                
                List<Map<String, Object>> stepsExecuted = new ArrayList<>();
                for (Map<String, Object> sd : stepDetails) {
                    Map<String, Object> se = new HashMap<>();
                    se.put("meter_point_id", sd.get("meter_point_id"));
                    se.put("step", sd.get("step"));
                    se.put("kwh_consumed", sd.get("kwh"));
                    se.put("unit_price", sd.get("price"));
                    se.put("amount", sd.get("amount"));
                    stepsExecuted.add(se);
                }
                breakdown.put("steps_executed", stepsExecuted);
                breakdown.put("total_before_tax", totalBeforeTax);
                manifest.put("rating_breakdown", breakdown);

                Map<String, Object> taxCalc = new HashMap<>();
                taxCalc.put("vat_rate", 0.10);
                taxCalc.put("tax_amount_raw", taxAmount);
                taxCalc.put("rounding_mode", "HALF_UP");
                taxCalc.put("tax_amount_final", taxAmount);
                manifest.put("tax_calculation", taxCalc);
                
                manifest.put("total_final_amount", totalAfterTax);

                String manifestJson = objectMapper.writeValueAsString(manifest);
                String idempotencyKey = accountId + "_" + month + "_p" + task.getPeriod() + "_v" + version;
                boolean isProrated = (BigDecimal.valueOf(daysUsed).compareTo(BigDecimal.valueOf(daysInMonth)) < 0) && daysUsed > 0;

                // Add to invoice batch params
                invoiceBatch.add(new Object[] {
                    invoiceId,
                    accountId,
                    bookId,
                    month,
                    task.getPeriod(),
                    totalBeforeTax,
                    taxAmount,
                    totalAfterTax,
                    idempotencyKey,
                    manifestJson,
                    isProrated,
                    accountId + "_" + month + "_p" + task.getPeriod() + "_v" + version,
                    "FINAL",
                    java.sql.Timestamp.valueOf(LocalDateTime.now()),
                    java.sql.Timestamp.valueOf(LocalDateTime.now())
                });

                // Save Outbox Event
                Map<String, Object> outboxPayload = new HashMap<>();
                outboxPayload.put("invoiceId", invoiceId);
                outboxPayload.put("accountId", accountId);
                outboxPayload.put("billingCycleMonth", month);
                outboxPayload.put("amountBeforeTax", totalBeforeTax);
                outboxPayload.put("taxAmount", taxAmount);
                outboxPayload.put("amountAfterTax", totalAfterTax);
                outboxPayload.put("timestamp", LocalDateTime.now().toString());

                outboxBatch.add(new Object[] {
                    UUID.randomUUID(), // event_id
                    "INVOICE",
                    invoiceId,
                    "INVOICE_CREATED",
                    objectMapper.writeValueAsString(outboxPayload),
                    java.sql.Timestamp.valueOf(LocalDateTime.now())
                });

                // Status success batch
                statusBatch.add(new Object[] {
                    accountId,
                    month,
                    bookId,
                    task.getPeriod(),
                    "SUCCESS",
                    invoiceId,
                    null,
                    java.sql.Timestamp.valueOf(LocalDateTime.now())
                });

                // Enqueue success log
                String inputJson = objectMapper.writeValueAsString(consumptions);
                billingLogService.enqueueLog(bookId, accountId, month, task.getPeriod(), "SUCCESS", inputJson, manifestJson, null);

            } catch (Exception e) {
                log.error("Calculation failed for account: {}, error: {}", accountId, e.getMessage(), e);
                // Status fail batch
                statusBatch.add(new Object[] {
                    accountId,
                    month,
                    bookId,
                    task.getPeriod(),
                    "FAILED",
                    null,
                    e.getMessage(),
                    java.sql.Timestamp.valueOf(LocalDateTime.now())
                });
                // Enqueue failed log
                billingLogService.enqueueLog(bookId, accountId, month, task.getPeriod(), "FAILED", null, null, e.getMessage());
            }
        }

        // 6. Execute atomic Batch UPSERT on Citus/TiDB
        if (!invoiceBatch.isEmpty()) {
            String insertInvoiceSql = "INSERT INTO bill_invoice (" +
                    "invoice_id, account_id, book_id, billing_cycle_month, period, " +
                    "total_amount_before_tax, tax_amount, total_amount_after_tax, " +
                    "idempotency_key, billing_manifest, proration_applied, " +
                    "snapshot_ref, calculation_status, created_at, updated_at) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?, ?, ?, ?, ?) " +
                    "ON CONFLICT (idempotency_key) " +
                    "DO UPDATE SET " +
                    "total_amount_before_tax = EXCLUDED.total_amount_before_tax, " +
                    "tax_amount              = EXCLUDED.tax_amount, " +
                    "total_amount_after_tax  = EXCLUDED.total_amount_after_tax, " +
                    "billing_manifest        = EXCLUDED.billing_manifest, " +
                    "proration_applied       = EXCLUDED.proration_applied, " +
                    "snapshot_ref            = EXCLUDED.snapshot_ref, " +
                    "calculation_status      = EXCLUDED.calculation_status, " +
                    "updated_at              = NOW()";
            
            String insertOutboxSql = "INSERT INTO outbox_event (event_id, aggregate_type, aggregate_id, event_type, payload, status, created_at) " +
                    "VALUES (?, ?, ?, ?, ?::jsonb, 'PENDING', ?)";

            jdbcTemplate.batchUpdate(insertInvoiceSql, invoiceBatch);
            jdbcTemplate.batchUpdate(insertOutboxSql, outboxBatch);
            
            log.info("[AUDIT-TRACER] Batch transaction committed. Saved {} invoices & outbox events to Postgres.", invoiceBatch.size());
        }

        // 7. Write run states
        if (!statusBatch.isEmpty()) {
            String insertStatusSql = "INSERT INTO account_billing_status (account_id, billing_cycle_month, book_id, period, status, invoice_id, error_message, updated_at) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?) " +
                    "ON CONFLICT (account_id, billing_cycle_month, period) DO UPDATE SET " +
                    "status = EXCLUDED.status, invoice_id = EXCLUDED.invoice_id, error_message = EXCLUDED.error_message, updated_at = EXCLUDED.updated_at";
            jdbcTemplate.batchUpdate(insertStatusSql, statusBatch);

            String hashKey = "billing:book_status_hash:" + firstBookId + ":" + firstMonth;
            Map<String, String> localMap = localBookStatusCache.get(firstBookId + ":" + firstMonth);
            Map<String, String> redisUpdates = new HashMap<>();
            
            int processedDelta = 0;
            int successDelta = 0;
            int failedDelta = 0;

            for (Object[] row : statusBatch) {
                String accId = (String) row[0];
                String stat = (String) row[3];

                if (localMap != null) {
                    localMap.put(accId, stat);
                }
                redisUpdates.put(accId, stat);

                processedDelta++;
                if ("SUCCESS".equals(stat)) {
                    successDelta++;
                } else {
                    failedDelta++;
                }
            }

            try {
                if (!redisUpdates.isEmpty()) {
                    redisTemplate.opsForHash().putAll(hashKey, redisUpdates);
                }
            } catch (Exception e) {
                // Ignore
            }

            updateBookBillingRunProgress(firstBookId, firstMonth, tasks.get(0).getPeriod(), processedDelta, successDelta, failedDelta);
            log.info("[AUDIT-TRACER] Persistent Billing Status written for {} accounts. Success: {}, Failed: {}.", processedDelta, successDelta, failedDelta);
        }
    }

    private void validateSnapshot(BillingConfigSnapshot config, String accountId) {
        if (config == null) {
            throw new com.evn.billing.worker.exception.MalformSnapshotException("Snapshot config is null for account: " + accountId);
        }
        if (config.getAccountId() == null || config.getAccountId().isEmpty()) {
            throw new com.evn.billing.worker.exception.MalformSnapshotException("Missing accountId in snapshot config for account: " + accountId);
        }
        if (config.getBookId() == null || config.getBookId().isEmpty()) {
            throw new com.evn.billing.worker.exception.MalformSnapshotException("Missing bookId in snapshot config for account: " + accountId);
        }
        if (config.getEffectiveSyncDate() == null) {
            throw new com.evn.billing.worker.exception.MalformSnapshotException("Missing effectiveSyncDate in snapshot config for account: " + accountId);
        }
        if (config.getMeterTopology() == null || config.getMeterTopology().getRootPoints() == null || config.getMeterTopology().getRootPoints().isEmpty()) {
            throw new com.evn.billing.worker.exception.MalformSnapshotException("Missing or empty meterTopology in snapshot config for account: " + accountId);
        }
        if (config.getTariffs() == null || config.getTariffs().isEmpty()) {
            throw new com.evn.billing.worker.exception.MalformSnapshotException("Missing or empty tariffs in snapshot config for account: " + accountId);
        }
    }

    private String getCalculationType(BillingConfigSnapshot config, String meterPointId) {
        if (config == null || config.getMeterTopology() == null || config.getMeterTopology().getRootPoints() == null) {
            return "UNKNOWN";
        }
        for (MeterPointNode root : config.getMeterTopology().getRootPoints()) {
            String type = findCalculationType(root, meterPointId);
            if (type != null) {
                return type;
            }
        }
        return "UNKNOWN";
    }

    private String findCalculationType(MeterPointNode node, String meterPointId) {
        if (meterPointId.equals(node.getMeterPointId())) {
            return node.getCalculationType() != null ? node.getCalculationType().name() : "AGGREGATION";
        }
        if (node.getChildPoints() != null) {
            for (MeterPointNode child : node.getChildPoints()) {
                String type = findCalculationType(child, meterPointId);
                if (type != null) {
                    return type;
                }
            }
        }
        return null;
    }

    @Transactional
    public void cancelBilling(String accountId, String month, int period) throws Exception {
        Optional<AccountBillingStatus> statusOpt = accountBillingStatusRepository
                .findById(new AccountBillingStatusId(accountId, month, period));
        if (statusOpt.isEmpty()) {
            throw new NoSuchElementException("Không tìm thấy thông tin cước đã tính cho khách hàng: " + accountId + ", kỳ: " + month + ", đợt: " + period);
        }
        
        AccountBillingStatus currentStatus = statusOpt.get();
        String bookId = currentStatus.getBookId();
        String oldStatus = currentStatus.getStatus();

        log.info("[CANCEL-BILL] Cancelling billing for Account: {}, Month: {}, Period: {}, Book: {}, Old Status: {}", 
                accountId, month, period, bookId, oldStatus);

        jdbcTemplate.update("DELETE FROM bill_invoice WHERE account_id = ? AND billing_cycle_month = ? AND period = ?", accountId, month, period);
        log.info("[CANCEL-BILL] Deleted invoices from 'bill_invoice' table.");

        jdbcTemplate.update("DELETE FROM billing_calculation_log WHERE account_id = ? AND billing_cycle_month = ? AND period = ?", accountId, month, period);
        log.info("[CANCEL-BILL] Deleted logs from 'billing_calculation_log' table.");

        currentStatus.setStatus("CANCELLED");
        currentStatus.setInvoiceId(null);
        currentStatus.setErrorMessage("Hủy hóa đơn bởi người vận hành");
        currentStatus.setUpdatedAt(LocalDateTime.now());
        accountBillingStatusRepository.save(currentStatus);

        String hashKey = "billing:book_status_hash:" + bookId + ":" + month + ":" + period;
        try {
            redisTemplate.opsForHash().put(hashKey, accountId, "CANCELLED");
        } catch (Exception e) {
            log.warn("[CANCEL-BILL] Failed to update Redis status to CANCELLED: {}", e.getMessage());
        }

        String localKey = bookId + ":" + month + ":" + period;
        Map<String, String> localMap = localBookStatusCache.get(localKey);
        if (localMap != null) {
            localMap.put(accountId, "CANCELLED");
        }

        if ("SUCCESS".equals(oldStatus)) {
            updateBookBillingRunProgress(bookId, month, period, -1, -1, 0);
        } else if ("FAILED".equals(oldStatus)) {
            updateBookBillingRunProgress(bookId, month, period, -1, 0, -1);
        }
        log.info("[CANCEL-BILL] Billing run progress decremented.");
    }
}
