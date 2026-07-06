package com.evn.billing.worker.service;

import com.evn.billing.common.domain.BillInvoice;
import com.evn.billing.common.domain.BillingAccountSnapshot;
import com.evn.billing.common.domain.MeterUsage;
import com.evn.billing.common.domain.OutboxEvent;
import com.evn.billing.common.domain.AccountBillingStatus;
import com.evn.billing.common.domain.AccountBillingStatusId;
import com.evn.billing.common.domain.BookBillingRun;
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
import com.evn.billing.worker.repository.BookBillingRunRepository;
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
    private BookBillingRunRepository bookBillingRunRepository;

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

    public void warmupBookCache(String bookId, String month) {
        if (bookId == null || bookId.isEmpty()) return;
        String cacheWarmedKey = "billing:book_warmed:" + bookId + ":" + month;
        String hashKey = "billing:book_status_hash:" + bookId + ":" + month;
        String localKey = bookId + ":" + month;

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
            log.info("[WARMUP] Cache miss for Book: {}, Month: {}. Loading from Postgres...", bookId, month);
            List<AccountBillingStatus> dbStatuses = accountBillingStatusRepository.findByBookIdAndBillingCycleMonth(bookId, month);
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
            log.info("[WARMUP] Cache warmed for Book: {}, Month: {}. Loaded {} statuses.", bookId, month, statusMap.size());
        }

        Map<String, String> localMap = new java.util.concurrent.ConcurrentHashMap<>();
        try {
            Map<Object, Object> redisHash = redisTemplate.opsForHash().entries(hashKey);
            for (Map.Entry<Object, Object> entry : redisHash.entrySet()) {
                localMap.put(entry.getKey().toString(), entry.getValue().toString());
            }
        } catch (Exception e) {
            log.warn("[WARMUP] Redis lookup failed, loading locally from Postgres for thread safety.");
            List<AccountBillingStatus> dbStatuses = accountBillingStatusRepository.findByBookIdAndBillingCycleMonth(bookId, month);
            for (AccountBillingStatus abs : dbStatuses) {
                localMap.put(abs.getAccountId(), abs.getStatus());
            }
        }
        localBookStatusCache.put(localKey, localMap);
        log.info("[WARMUP] JVM Memory loaded for Book: {}, Month: {}. Total in-memory keys: {}", bookId, month, localMap.size());
    }

    public String getAccountStatus(String bookId, String accountId, String month) {
        if (bookId == null || bookId.isEmpty()) return null;
        String localKey = bookId + ":" + month;
        Map<String, String> localMap = localBookStatusCache.get(localKey);
        if (localMap != null && localMap.containsKey(accountId)) {
            return localMap.get(accountId);
        }

        String hashKey = "billing:book_status_hash:" + bookId + ":" + month;
        try {
            Object val = redisTemplate.opsForHash().get(hashKey, accountId);
            if (val != null) {
                return val.toString();
            }
        } catch (Exception e) {
            // Ignore
        }

        Optional<AccountBillingStatus> dbStatus = accountBillingStatusRepository
                .findById(new AccountBillingStatusId(accountId, month));
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

    public void updateAccountStatus(String bookId, String accountId, String month, String status, String invoiceId, String errorMsg) {
        if (bookId == null || bookId.isEmpty()) return;
        AccountBillingStatus abs = new AccountBillingStatus();
        abs.setAccountId(accountId);
        abs.setBillingCycleMonth(month);
        abs.setBookId(bookId);
        abs.setStatus(status);
        abs.setInvoiceId(invoiceId);
        abs.setErrorMessage(errorMsg);
        abs.setUpdatedAt(LocalDateTime.now());
        accountBillingStatusRepository.save(abs);

        String hashKey = "billing:book_status_hash:" + bookId + ":" + month;
        try {
            redisTemplate.opsForHash().put(hashKey, accountId, status);
        } catch (Exception e) {
            // Ignore
        }

        String localKey = bookId + ":" + month;
        Map<String, String> localMap = localBookStatusCache.get(localKey);
        if (localMap != null) {
            localMap.put(accountId, status);
        }
    }

    public void updateBookBillingRunProgress(String bookId, String month, int processedDelta, int successDelta, int failedDelta) {
        if (bookId == null || bookId.isEmpty()) return;
        String sql = "UPDATE book_billing_run SET " +
                "processed_accounts = processed_accounts + ?, " +
                "success_accounts = success_accounts + ?, " +
                "failed_accounts = failed_accounts + ?, " +
                "updated_at = NOW() " +
                "WHERE book_id = ? AND billing_cycle_month = ?";
        jdbcTemplate.update(sql, processedDelta, successDelta, failedDelta, bookId, month);
    }

    /**
     * Executes the rating calculations for a specific customer account in the batch cycle,
     * saving the resulting Invoice and the Outbox Event atomically.
     * 
     * @param task The task payload consumed from Kafka containing account metadata.
     */
    @Transactional
    public void processBilling(BillingTaskDto task) throws Exception {
        String accountId = task.getAccountId();
        String month = task.getBillingCycleMonth();
        int version = task.getCalculationVersion();
        String bookId = task.getBookId() != null ? task.getBookId() : "DEMAND";

        // 0. Kiểm tra trạng thái cước (Skip if already SUCCESS)
        warmupBookCache(bookId, month);
        String currentStatus = getAccountStatus(bookId, accountId, month);
        if ("SUCCESS".equals(currentStatus)) {
            log.info("[SKIP-CALC] Account {} is already calculated successfully for month {}. Aborting on-demand calculation.", accountId, month);
            throw new IllegalStateException("Khách hàng đã được tính cước thành công. Vui lòng hủy hóa đơn cũ trước khi tính lại.");
        }

        try {
            // 1. Lấy chỉ số sử dụng đo đếm đã hợp lệ (VALIDATED)
            List<MeterUsage> usages = meterUsageRepository.findByAccountIdAndBillingCycleMonthAndStatus(accountId, month, "VALIDATED");
            if (usages.isEmpty()) {
                throw new NoSuchElementException("No validated meter usage found for account: " + accountId);
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

            // 2. Lấy dữ liệu đóng băng Snapshot từ Redis Cache (Cache-aside)
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
                        .findByAccountIdAndBillingCycleMonthAndCalculationVersion(accountId, month, version);
                if (snapshotOpt.isEmpty()) {
                    throw new NoSuchElementException("No snapshot profile found for account: " + accountId + ", version: " + version);
                }
                config = snapshotOpt.get().getConfigData();
                // Cập nhật lại vào cache Redis
                try {
                    redisTemplate.opsForValue().set(cacheKey, config, 24, TimeUnit.HOURS);
                } catch (Exception e) {
                    // Bỏ qua lỗi ghi cache
                }
            }

            log.info("[AUDIT-TRACER] [Account: {}] Step 4: Kafka calculation task received. Triggering billing engine processing.", accountId);

            // 3. Tính toán sản lượng thực tế thô từ database
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

            // 4. Gọi bộ tính cước Master để thực thi tính toán
            CalculationResult result = billingCalculator.calculate(config, consumptions, month, daysUsed);

            BigDecimal totalBeforeTax = result.getTotalAmountBeforeTax();
            BigDecimal taxAmount = result.getTaxAmount();
            BigDecimal totalAfterTax = result.getTotalAmountAfterTax();
            Map<String, Object> meterPointBreakdowns = result.getMeterPointBreakdowns();
            List<Map<String, Object>> stepDetails = result.getStepDetails();
            Map<String, BigDecimal> nodeNetConsumptions = result.getNodeNetConsumptions();

            log.info("[AUDIT-TRACER] [Account: {}] Step 5.1: Billing engine finished. Total Net Consumption: {}, Total Amount before tax: {}, VAT Tax Amount: {}, Total Amount after tax: {}.", 
                    accountId, nodeNetConsumptions, totalBeforeTax, taxAmount, totalAfterTax);
            log.info("[AUDIT-TRACER] [Account: {}] Step 5.2: Stepping calculation tier breakdown:", accountId);
            for (Map<String, Object> sd : stepDetails) {
                log.info("  - Meter: {}, Step: {}, Consumption: {} kWh, Price: {}đ, Amount: {}đ", 
                        sd.get("meter_point_id"), sd.get("step"), sd.get("kwh"), sd.get("price"), sd.get("amount"));
            }

            // 5. Xây dựng tài liệu giải trình kiểm toán billing_manifest
            Map<String, Object> manifest = new HashMap<>();
            manifest.put("invoice_id", "INV-" + accountId + "-" + month + "-v" + version);
            
            Map<String, Object> auditTrail = new HashMap<>();
            auditTrail.put("engine_version", "v1.0.0-prod");
            auditTrail.put("execution_timestamp", LocalDateTime.now().toString());
            auditTrail.put("snapshot_applied", accountId + "_" + month + "_v" + version);
            manifest.put("audit_trail", auditTrail);

            Map<String, Object> topologyCalculation = new HashMap<>();
            List<Map<String, Object>> inputReadings = new ArrayList<>();
            for (MeterUsage u : usages) {
                Map<String, Object> ir = new HashMap<>();
                ir.put("meter_point_id", u.getMeterPointId());
                ir.put("kwh", u.getConsumption() != null ? u.getConsumption() : u.getEndIndex().subtract(u.getStartIndex()));
                inputReadings.add(ir);
            }
            topologyCalculation.put("input_readings", inputReadings);
            topologyCalculation.put("node_net_consumptions", nodeNetConsumptions);
            manifest.put("topology_calculation", topologyCalculation);

            Map<String, Object> breakdown = new HashMap<>();
            breakdown.put("norms_factor_applied", config.getNormsFactor());
            breakdown.put("meter_point_breakdowns", meterPointBreakdowns);
            breakdown.put("total_before_tax", totalBeforeTax);
            manifest.put("rating_breakdown", breakdown);

            String manifestJson = objectMapper.writeValueAsString(manifest);

            // 6. Khởi tạo & Ghi hóa đơn
            BillInvoice invoice = new BillInvoice();
            String invoiceId = "INV-" + accountId + "-" + month + "-v" + version;
            invoice.setInvoiceId(invoiceId);
            invoice.setBillingCycleMonth(month);
            invoice.setAccountId(accountId);
            invoice.setBookId(task.getBookId());
            invoice.setTotalAmountBeforeTax(totalBeforeTax);
            invoice.setTaxAmount(taxAmount);
            invoice.setTotalAmountAfterTax(totalAfterTax);
            
            String idempotencyKey = accountId + "_" + month + "_v" + version;
            invoice.setIdempotencyKey(idempotencyKey);
            invoice.setBillingManifest(manifestJson);
            invoice.setCreatedAt(LocalDateTime.now());

            billInvoiceRepository.save(invoice);
            log.info("[AUDIT-TRACER] [Account: {}] Step 6: Database transaction committed. Invoice saved to 'bill_invoice' table with ID '{}'.", accountId, invoiceId);

            // 7. Ghi sự kiện Outbox (Transactional Outbox Pattern)
            Map<String, Object> outboxPayload = new HashMap<>();
            outboxPayload.put("invoiceId", invoiceId);
            outboxPayload.put("accountId", accountId);
            outboxPayload.put("billingCycleMonth", month);
            outboxPayload.put("amountBeforeTax", totalBeforeTax);
            outboxPayload.put("taxAmount", taxAmount);
            outboxPayload.put("amountAfterTax", totalAfterTax);
            outboxPayload.put("timestamp", LocalDateTime.now().toString());

            OutboxEvent event = new OutboxEvent();
            event.setAggregateType("INVOICE");
            event.setAggregateId(invoiceId);
            event.setEventType("INVOICE_CREATED");
            event.setPayload(objectMapper.writeValueAsString(outboxPayload));
            event.setCreatedAt(LocalDateTime.now());

            outboxEventRepository.save(event);
            log.info("[AUDIT-TRACER] [Account: {}] Step 6.1: Outbox event 'INVOICE_CREATED' created. Ready for CDC downstream replication.", accountId);

            // Cập nhật trạng thái tính cước thành công lâu dài
            updateAccountStatus(bookId, accountId, month, "SUCCESS", invoiceId, null);
            updateBookBillingRunProgress(bookId, month, 1, 1, 0);

            // Enqueue success log
            String inputJson = objectMapper.writeValueAsString(consumptions);
            billingLogService.enqueueLog(task.getBookId(), accountId, month, "SUCCESS", inputJson, manifestJson, null);
        } catch (Exception e) {
            updateAccountStatus(bookId, accountId, month, "FAILED", null, e.getMessage());
            updateBookBillingRunProgress(bookId, month, 1, 0, 1);
            billingLogService.enqueueLog(task.getBookId(), accountId, month, "FAILED", null, null, e.getMessage());
            throw e;
        }
    }
    @Transactional
    public void processBillingBatch(List<BillingTaskDto> tasks) throws Exception {
        if (tasks == null || tasks.isEmpty()) return;

        String firstBookId = tasks.get(0).getBookId() != null ? tasks.get(0).getBookId() : "DEMAND";
        String firstMonth = tasks.get(0).getBillingCycleMonth();
        
        // 0. Khởi động làm nóng cache (Warmup) cho Sổ và Kỳ cước
        warmupBookCache(firstBookId, firstMonth);

        List<Object[]> invoiceBatch = new ArrayList<>();
        List<Object[]> outboxBatch = new ArrayList<>();
        List<Object[]> statusBatch = new ArrayList<>();

        for (BillingTaskDto task : tasks) {
            String accountId = task.getAccountId();
            String month = task.getBillingCycleMonth();
            int version = task.getCalculationVersion();
            String bookId = task.getBookId() != null ? task.getBookId() : "DEMAND";

            // 0.1. Kiểm tra trạng thái cước (Skip if already SUCCESS)
            String currentStatus = getAccountStatus(bookId, accountId, month);
            if ("SUCCESS".equals(currentStatus)) {
                log.info("[SKIP-CALC-BATCH] Account {} is already calculated successfully for month {}. Skipping computation in batch...", accountId, month);
                continue;
            }

            try {
                // 1. Lấy chỉ số sử dụng đo đếm đã hợp lệ (VALIDATED)
                List<MeterUsage> usages = meterUsageRepository.findByAccountIdAndBillingCycleMonthAndStatus(accountId, month, "VALIDATED");
                if (usages.isEmpty()) {
                    throw new NoSuchElementException("No validated meter usage found for account: " + accountId);
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

                // 2. Lấy dữ liệu đóng băng Snapshot từ Redis Cache (Cache-aside)
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
                            .findByAccountIdAndBillingCycleMonthAndCalculationVersion(accountId, month, version);
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

                // 3. Tính toán sản lượng thực tế thô từ database
                Map<String, BigDecimal> consumptions = new HashMap<>();
                for (MeterUsage u : usages) {
                    BigDecimal cons = u.getConsumption() != null ? u.getConsumption() 
                            : u.getEndIndex().subtract(u.getStartIndex());
                    consumptions.put(u.getMeterPointId(), cons);
                }

                // 4. Gọi bộ tính cước Master để thực thi tính toán
                CalculationResult result = billingCalculator.calculate(config, consumptions, month, daysUsed);

                BigDecimal totalBeforeTax = result.getTotalAmountBeforeTax();
                BigDecimal taxAmount = result.getTaxAmount();
                BigDecimal totalAfterTax = result.getTotalAmountAfterTax();
                Map<String, Object> meterPointBreakdowns = result.getMeterPointBreakdowns();
                Map<String, BigDecimal> nodeNetConsumptions = result.getNodeNetConsumptions();

                // 5. Xây dựng tài liệu giải trình kiểm toán billing_manifest
                Map<String, Object> manifest = new HashMap<>();
                String invoiceId = "INV-" + accountId + "-" + month + "-v" + version;
                manifest.put("invoice_id", invoiceId);
                
                Map<String, Object> auditTrail = new HashMap<>();
                auditTrail.put("engine_version", "v1.0.0-prod");
                auditTrail.put("execution_timestamp", LocalDateTime.now().toString());
                auditTrail.put("snapshot_applied", accountId + "_" + month + "_v" + version);
                manifest.put("audit_trail", auditTrail);

                Map<String, Object> topologyCalculation = new HashMap<>();
                List<Map<String, Object>> inputReadings = new ArrayList<>();
                for (MeterUsage u : usages) {
                    Map<String, Object> ir = new HashMap<>();
                    ir.put("meter_point_id", u.getMeterPointId());
                    ir.put("kwh", u.getConsumption() != null ? u.getConsumption() : u.getEndIndex().subtract(u.getStartIndex()));
                    inputReadings.add(ir);
                }
                topologyCalculation.put("input_readings", inputReadings);
                topologyCalculation.put("node_net_consumptions", nodeNetConsumptions);
                manifest.put("topology_calculation", topologyCalculation);

                Map<String, Object> breakdown = new HashMap<>();
                breakdown.put("norms_factor_applied", config.getNormsFactor());
                breakdown.put("meter_point_breakdowns", meterPointBreakdowns);
                breakdown.put("total_before_tax", totalBeforeTax);
                manifest.put("rating_breakdown", breakdown);

                String manifestJson = objectMapper.writeValueAsString(manifest);

                // Invoice batch params
                String idempotencyKey = accountId + "_" + month + "_v" + version;
                invoiceBatch.add(new Object[] {
                    invoiceId,
                    accountId,
                    bookId,
                    month,
                    totalBeforeTax,
                    taxAmount,
                    totalAfterTax,
                    idempotencyKey,
                    manifestJson,
                    java.sql.Timestamp.valueOf(LocalDateTime.now())
                });

                // Outbox event payload & batch params
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

                // Trạng thái cước thành công
                statusBatch.add(new Object[] {
                    accountId,
                    month,
                    bookId,
                    "SUCCESS",
                    invoiceId,
                    null,
                    java.sql.Timestamp.valueOf(LocalDateTime.now())
                });

                // Enqueue async success log (inputs and output manifest json)
                String inputJson = objectMapper.writeValueAsString(consumptions);
                billingLogService.enqueueLog(bookId, accountId, month, "SUCCESS", inputJson, manifestJson, null);

            } catch (Exception e) {
                log.error("Calculation failed for account: {}, error: {}", accountId, e.getMessage(), e);
                // Trạng thái cước thất bại
                statusBatch.add(new Object[] {
                    accountId,
                    month,
                    bookId,
                    "FAILED",
                    null,
                    e.getMessage(),
                    java.sql.Timestamp.valueOf(LocalDateTime.now())
                });
                // Enqueue async failed log
                billingLogService.enqueueLog(bookId, accountId, month, "FAILED", null, null, e.getMessage());
            }
        }

        // 6. Thực thi ghi lô vào CSDL Postgres (Batch Insert)
        if (!invoiceBatch.isEmpty()) {
            String insertInvoiceSql = "INSERT INTO bill_invoice (invoice_id, account_id, book_id, billing_cycle_month, total_amount_before_tax, tax_amount, total_amount_after_tax, idempotency_key, billing_manifest, created_at) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?) ON CONFLICT (invoice_id, billing_cycle_month) DO NOTHING";
            
            String insertOutboxSql = "INSERT INTO outbox_event (event_id, aggregate_type, aggregate_id, event_type, payload, created_at) " +
                    "VALUES (?, ?, ?, ?, ?::jsonb, ?)";

            jdbcTemplate.batchUpdate(insertInvoiceSql, invoiceBatch);
            jdbcTemplate.batchUpdate(insertOutboxSql, outboxBatch);
            
            log.info("[AUDIT-TRACER] Batch transaction committed. Saved {} invoices & outbox events to Postgres.", invoiceBatch.size());
        }

        // 7. Thực thi ghi lô trạng thái cước vào CSDL Postgres
        if (!statusBatch.isEmpty()) {
            String insertStatusSql = "INSERT INTO account_billing_status (account_id, billing_cycle_month, book_id, status, invoice_id, error_message, updated_at) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?) " +
                    "ON CONFLICT (account_id, billing_cycle_month) DO UPDATE SET " +
                    "status = EXCLUDED.status, invoice_id = EXCLUDED.invoice_id, error_message = EXCLUDED.error_message, updated_at = EXCLUDED.updated_at";
            jdbcTemplate.batchUpdate(insertStatusSql, statusBatch);

            // Đồng bộ trạng thái lên Redis và JVM Cache
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

            // Cập nhật tiến độ Sổ chốt cước
            updateBookBillingRunProgress(firstBookId, firstMonth, processedDelta, successDelta, failedDelta);
            log.info("[AUDIT-TRACER] Persistent Billing Status written for {} accounts. Success: {}, Failed: {}.", processedDelta, successDelta, failedDelta);
        }
    }

    @Transactional
    public void cancelBilling(String accountId, String month) throws Exception {
        Optional<AccountBillingStatus> statusOpt = accountBillingStatusRepository
                .findById(new AccountBillingStatusId(accountId, month));
        if (statusOpt.isEmpty()) {
            throw new NoSuchElementException("Không tìm thấy thông tin cước đã tính cho khách hàng: " + accountId + ", kỳ: " + month);
        }
        
        AccountBillingStatus currentStatus = statusOpt.get();
        String bookId = currentStatus.getBookId();
        String oldStatus = currentStatus.getStatus();

        log.info("[CANCEL-BILL] Cancelling billing for Account: {}, Month: {}, Book: {}, Old Status: {}", 
                accountId, month, bookId, oldStatus);

        // Delete Invoice from database
        jdbcTemplate.update("DELETE FROM bill_invoice WHERE account_id = ? AND billing_cycle_month = ?", accountId, month);
        log.info("[CANCEL-BILL] Deleted invoices from 'bill_invoice' table.");

        // Delete calculation logs
        jdbcTemplate.update("DELETE FROM billing_calculation_log WHERE account_id = ? AND billing_cycle_month = ?", accountId, month);
        log.info("[CANCEL-BILL] Deleted logs from 'billing_calculation_log' table.");

        // Update status to CANCELLED
        currentStatus.setStatus("CANCELLED");
        currentStatus.setInvoiceId(null);
        currentStatus.setErrorMessage("Hủy hóa đơn bởi người vận hành");
        currentStatus.setUpdatedAt(LocalDateTime.now());
        accountBillingStatusRepository.save(currentStatus);

        // Update Redis Hash
        String hashKey = "billing:book_status_hash:" + bookId + ":" + month;
        try {
            redisTemplate.opsForHash().put(hashKey, accountId, "CANCELLED");
        } catch (Exception e) {
            log.warn("[CANCEL-BILL] Failed to update Redis status to CANCELLED: {}", e.getMessage());
        }

        // Update JVM Local Cache
        String localKey = bookId + ":" + month;
        Map<String, String> localMap = localBookStatusCache.get(localKey);
        if (localMap != null) {
            localMap.put(accountId, "CANCELLED");
        }

        // Update progress count
        if ("SUCCESS".equals(oldStatus)) {
            updateBookBillingRunProgress(bookId, month, -1, -1, 0);
        } else if ("FAILED".equals(oldStatus)) {
            updateBookBillingRunProgress(bookId, month, -1, 0, -1);
        }
        log.info("[CANCEL-BILL] Billing run progress decremented.");
    }
}
