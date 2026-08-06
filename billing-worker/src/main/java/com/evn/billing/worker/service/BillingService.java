package com.evn.billing.worker.service;

import com.evn.billing.common.domain.BillingAccountSnapshot;
import com.evn.billing.common.domain.MeterUsage;
import com.evn.billing.common.domain.AccountBillingStatus;
import com.evn.billing.common.domain.AccountBillingStatusId;
import com.evn.billing.common.domain.DtuongQlySchedule;
import com.evn.billing.common.dto.BillingConfigSnapshot;
import com.evn.billing.common.dto.MeterPointNode;
import com.evn.billing.common.dto.BillingSchemaStep;
import com.evn.billing.common.domain.CauHinhThue;
import com.evn.billing.worker.repository.CauHinhThueRepository;
import com.evn.billing.engine.BillingCalculator;
import com.evn.billing.engine.CalculationResult;
import com.evn.billing.common.dto.BillingTaskDto;
import com.evn.billing.common.dto.MeterReadingDto;
import com.evn.billing.worker.repository.BillingAccountSnapshotRepository;
import com.evn.billing.worker.repository.BillingStateRepository;
import com.evn.billing.worker.repository.MeterUsageRepository;
import com.evn.billing.worker.repository.AccountBillingStatusRepository;
import com.evn.billing.worker.repository.DtuongQlyScheduleRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
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
    private AccountBillingStatusRepository accountBillingStatusRepository;

    @Autowired
    private DtuongQlyScheduleRepository dtuongQlyScheduleRepository;

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private BillingLogService billingLogService;

    @Autowired
    private BillingStateRepository billingStateRepository;
 
    @Autowired
    private org.springframework.kafka.core.KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private CauHinhThueRepository cauHinhThueRepository;

    @Autowired(required = false)
    private io.micrometer.core.instrument.MeterRegistry meterRegistry;

    private final BillingCalculator billingCalculator = new BillingCalculator();
 
    @Value("${billing.worker.claim-timeout-minutes:15}")
    private int claimTimeoutMinutes;
 
    @Value("${billing.worker.anomaly-threshold-vnd:1000000}")
    private double anomalyThresholdVnd;
 
    private final Map<String, Map<String, String>> localBookStatusCache = new java.util.concurrent.ConcurrentHashMap<>();

    private final String workerNodeId = initWorkerNodeId();

    private String initWorkerNodeId() {
        try {
            return java.net.InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "worker-" + java.util.UUID.randomUUID();
        }
    }

    private boolean tryClaimProcessingWorker(String accountId, String month, int period) {
        int updated = billingStateRepository.tryClaimProcessingWorker(workerNodeId, accountId, month, period, claimTimeoutMinutes);
        return updated > 0;
    }

    public void warmupBookCache(String dtuongQly, String month, int period) {
        if (dtuongQly == null || dtuongQly.isEmpty()) return;
        String cacheWarmedKey = "billing:book_warmed:" + dtuongQly + ":" + month + ":" + period;
        String hashKey = "billing:book_status_hash:" + dtuongQly + ":" + month + ":" + period;
        String localKey = dtuongQly + ":" + month + ":" + period;

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
            log.info("[WARMUP] Cache miss for Book: {}, Month: {}, Period: {}. Loading from Postgres...", dtuongQly, month, period);
            List<AccountBillingStatus> dbStatuses = accountBillingStatusRepository.findByDtuongQlyAndThangChuKyAndKyChot(dtuongQly, month, period);
            Map<String, String> statusMap = new HashMap<>();
            for (AccountBillingStatus abs : dbStatuses) {
                statusMap.put(abs.getMaKhang(), abs.getTrangThai());
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
            log.info("[WARMUP] Cache warmed for Book: {}, Month: {}, Period: {}. Loaded {} statuses.", dtuongQly, month, period, statusMap.size());
        }

        Map<String, String> localMap = new java.util.concurrent.ConcurrentHashMap<>();
        try {
            Map<Object, Object> redisHash = redisTemplate.opsForHash().entries(hashKey);
            for (Map.Entry<Object, Object> entry : redisHash.entrySet()) {
                localMap.put(entry.getKey().toString(), entry.getValue().toString());
            }
        } catch (Exception e) {
            log.warn("[WARMUP] Redis lookup failed, loading locally from Postgres for thread safety.");
            List<AccountBillingStatus> dbStatuses = accountBillingStatusRepository.findByDtuongQlyAndThangChuKyAndKyChot(dtuongQly, month, period);
            for (AccountBillingStatus abs : dbStatuses) {
                localMap.put(abs.getMaKhang(), abs.getTrangThai());
            }
        }
        localBookStatusCache.put(localKey, localMap);
        log.info("[WARMUP] JVM Memory loaded for Book: {}, Month: {}, Period: {}. Total in-memory keys: {}", dtuongQly, month, period, localMap.size());
    }

    public String getAccountStatus(String dtuongQly, String accountId, String month, int period) {
        if (dtuongQly == null || dtuongQly.isEmpty()) return null;
        String localKey = dtuongQly + ":" + month + ":" + period;
        Map<String, String> localMap = localBookStatusCache.get(localKey);
        if (localMap != null && localMap.containsKey(accountId)) {
            return localMap.get(accountId);
        }

        String hashKey = "billing:book_status_hash:" + dtuongQly + ":" + month + ":" + period;
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
            String dbVal = dbStatus.get().getTrangThai();
            try {
                redisTemplate.opsForHash().put(hashKey, accountId, dbVal);
            } catch (Exception e) {
                // Ignore
            }
            return dbVal;
        }

        return null;
    }

    public boolean updateAccountStatus(String dtuongQly, String accountId, String month, int period, String status, String invoiceId, String errorMsg, Long durationMs, String workerNode) {
        if (dtuongQly == null || dtuongQly.isEmpty()) return false;
        billingStateRepository.seedProcessingStatus(accountId, month, dtuongQly, period, workerNode);

        int affected = billingStateRepository.updateProcessingStatus(
                status,
                invoiceId,
                errorMsg,
                durationMs,
                workerNode,
                dtuongQly,
                accountId,
                month,
                period
        );

        if (affected == 0) {
            log.warn("[STATUS-GUARD] Skip status update for Account: {}, Month: {}, Period: {} -> {} because row is no longer claimable by worker {}.",
                accountId, month, period, status, workerNode);
            return false;
        }

        String hashKey = "billing:book_status_hash:" + dtuongQly + ":" + month + ":" + period;
        try {
            redisTemplate.opsForHash().put(hashKey, accountId, status);
        } catch (Exception e) {
            // Ignore
        }

        String localKey = dtuongQly + ":" + month + ":" + period;
        Map<String, String> localMap = localBookStatusCache.get(localKey);
        if (localMap != null) {
            localMap.put(accountId, status);
        }
        return true;
    }

    public void updateBookBillingRunProgress(String dtuongQly, String month, int period, int processedDelta, int successDelta, int failedDelta) {
        if (dtuongQly == null || dtuongQly.isEmpty()) return;
        billingStateRepository.updateBookBillingRunProgress(dtuongQly, month, period, processedDelta, successDelta, failedDelta);
    }

    public void calculateImmediate(String accountId, String month, Integer period, Integer version, String dtuongQly, String triggeredBy) throws Exception {
        int finalPeriod = period != null ? period : 1;
        int finalVersion = version != null ? version : 1;
        String finalDtuongQly = dtuongQly != null ? dtuongQly : "SO_DEMAND";
        String finalTriggeredBy = triggeredBy != null ? triggeredBy : "CMIS";

        BillingTaskDto task = new BillingTaskDto(accountId, finalDtuongQly, month, finalPeriod, finalVersion, "on_demand_trace");
        task.setTriggeredBy(finalTriggeredBy);
        processBilling(task);
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
        String accountId = task.getMaKhang();
        String month = task.getThangChuKy();
        int version = task.getPhienBanTinh();
        String dtuongQly = task.getDtuongQly() != null ? task.getDtuongQly() : "DEMAND";

        // 0. Claim processing ownership to prevent duplicate execution from retry/rebalance.
        if (!tryClaimProcessingWorker(accountId, month, task.getKyChot())) {
            log.info("[SKIP-CALC] Account {} is already being processed or finalized for month {} period {}.", accountId, month, task.getKyChot());
            return;
        }

        try {
            // 1. Get validated usages from Kafka payload to avoid DB read
            List<MeterUsage> usages = new ArrayList<>();
            if (task.getDanhSachChiSo() != null && !task.getDanhSachChiSo().isEmpty()) {
                for (MeterReadingDto r : task.getDanhSachChiSo()) {
                    MeterUsage u = new MeterUsage();
                    u.setMaDdo(r.getMaDdo());
                    u.setTuNgay(r.getTuNgay());
                    u.setDenNgay(r.getDenNgay());
                    u.setChiSoDau(r.getChiSoDau());
                    u.setChiSoCuoi(r.getChiSoCuoi());
                    u.setConsumption(r.getSanLuong());
                    u.setMaKhang(accountId);
                    u.setThangChuKy(month);
                    u.setTrangThaiXuLy("VALIDATED");
                    u.setCoQuayVong(r.getCoQuayVong() != null ? r.getCoQuayVong() : false);
                    u.setMaxRegisterSnapshot(r.getMaxRegisterSnapshot());
                    u.setLanDocPhu(r.getLanDocPhu() != null ? r.getLanDocPhu() : 1);
                    u.setLoaiGhiIndex(r.getLoaiGhiIndex() != null ? r.getLoaiGhiIndex() : "ORIGINAL");
                    u.setTgianBdien(r.getTgianBdien() != null ? r.getTgianBdien() : "KT");
                    usages.add(u);
                }
            } else {
                // Fallback to database query if no readings provided in Kafka payload (e.g. REST fallback)
                usages = meterUsageRepository.findByMaKhangAndThangChuKyAndKyChotAndTrangThaiXuLy(accountId, month, task.getKyChot(), "VALIDATED");
            }
            if (usages.isEmpty()) {
                throw new NoSuchElementException("No validated meter usage found/provided for account: " + accountId);
            }

            // Determine actual billing period length (daysUsed)
            LocalDateTime minFrom = null;
            LocalDateTime maxTo = null;
            for (MeterUsage u : usages) {
                if (u.getTuNgay() != null) {
                    if (minFrom == null || u.getTuNgay().isBefore(minFrom)) {
                        minFrom = u.getTuNgay();
                    }
                }
                if (u.getDenNgay() != null) {
                    if (maxTo == null || u.getDenNgay().isAfter(maxTo)) {
                        maxTo = u.getDenNgay();
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
            String cacheKey = "snapshot:" + accountId + ":" + month + ":" + task.getKyChot();
            BillingConfigSnapshot config = null;
            String changeFlags = task.getChangeFlags() != null ? task.getChangeFlags() : "NONE";
            
            if ("PRICE_CHANGE".equals(changeFlags) || "METER_CHANGE".equals(changeFlags) || "MULTI_CHANGE".equals(changeFlags)) {
                try {
                    redisTemplate.delete(cacheKey);
                    log.info("[SNAP-INVALIDATE] Invalidated snapshot cache due to config change flag: {}", changeFlags);
                } catch (Exception e) {
                    log.warn("Failed to invalidate snapshot cache: {}", e.getMessage());
                }
            } else {
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
            }

            if (config == null) {
                Optional<BillingAccountSnapshot> snapshotOpt = snapshotRepository
                        .findByMaKhangAndThangChuKyAndKyChotAndPhienBanTinh(accountId, month, task.getKyChot(), version);
                if (snapshotOpt.isEmpty()) {
                    // Fallback to version 1 if specific version not found
                    snapshotOpt = snapshotRepository
                            .findByMaKhangAndThangChuKyAndKyChotAndPhienBanTinh(accountId, month, task.getKyChot(), 1);
                }
                if (snapshotOpt.isEmpty()) {
                    throw new NoSuchElementException("No snapshot profile found for account: " + accountId + ", version: " + version);
                }
                config = snapshotOpt.get().getDuLieuCauHinh();
                try {
                    redisTemplate.opsForValue().set(cacheKey, config, 24, TimeUnit.HOURS);
                } catch (Exception e) {
                    // Ignore
                }
            }

            boolean isFastPath = "NONE".equals(changeFlags) 
                    && !task.isHasRelation() 
                    && config.isFastPathEnabled() 
                    && ("SINH_HOAT".equals(config.getCustomerType()) || "NGOAI_SINH_HOAT".equals(config.getCustomerType()));
            if (!isFastPath) {
                validateSnapshot(config, accountId);
            } else {
                log.info("[ROUTING-ENGINE] Skip snapshot validation check for Fast-Path account: {}", accountId);
            }

            log.info("[AUDIT-TRACER] [Account: {}] Step 4: Kafka calculation task received. Triggering billing engine processing.", accountId);

            // 3. Collect node consumptions (multi-BCS aware: aggregate active power, exclude VC)
            Map<String, BigDecimal> consumptions = new HashMap<>();
            for (MeterUsage u : usages) {
                BigDecimal cons = u.getConsumption() != null ? u.getConsumption() 
                        : u.getChiSoCuoi().subtract(u.getChiSoDau());
                String tp = u.getTgianBdien() != null ? u.getTgianBdien() : "KT";
                
                // Detail key per BCS register: METER-001_BT, METER-001_CD, METER-001_TD
                String detailKey = u.getMaDdo() + "_" + tp;
                consumptions.merge(detailKey, cons, BigDecimal::add);
                
                // Aggregate active power per meterPoint (exclude reactive power VC)
                if (!"VC".equals(tp)) {
                    consumptions.merge(u.getMaDdo(), cons, BigDecimal::add);
                }
            }

            BigDecimal proRataFactor = BigDecimal.ONE;
            if (daysUsed < daysInMonth && daysUsed > 0) {
                proRataFactor = BigDecimal.valueOf(daysUsed).divide(BigDecimal.valueOf(daysInMonth), 8, RoundingMode.HALF_UP);
            }

            // Dynamics fetch VAT tax rate realtime from repository
            BigDecimal vatRate = BigDecimal.valueOf(0.08);
            try {
                Optional<CauHinhThue> thueOpt = cauHinhThueRepository.findByLoaiThue("VAT");
                if (thueOpt.isPresent()) {
                    vatRate = thueOpt.get().getThueSuat();
                }
            } catch (Exception e) {
                log.warn("[BILLING-WORKER] Failed to fetch tax rate from Repository, using fallback default 0.08 (8%): {}", e.getMessage());
            }

            if (config.getSchemaSteps() != null) {
                for (BillingSchemaStep step : config.getSchemaSteps()) {
                    if ("TAX".equals(step.getVariantName())) {
                        if (step.getStepConfig() == null) {
                            step.setStepConfig(new HashMap<>());
                        }
                        step.getStepConfig().put("taxRate", vatRate.doubleValue());
                        log.info("[BILLING-WORKER] Overrode taxRate for step {} with value: {}", step.getStepNumber(), vatRate);
                    }
                }
            }

            log.info("[AUDIT-TRACER] [Account: {}] Step 5: Billing engine started. Tariffs={}. Days in month: {} days. Pro-rata days used: {} days (Pro-rata factor: {}).", 
                    accountId, config.getBieuGia().keySet(), daysInMonth, daysUsed, proRataFactor);

            // 4. Invoke Core Stateless Rating Engine
            CalculationResult result;
            if (isFastPath) {
                log.info("[ROUTING-ENGINE] [Account: {}] Routing calculation to calculateFastPath.", accountId);
                result = billingCalculator.calculateFastPath(config, consumptions, month, daysUsed);
            } else {
                log.info("[ROUTING-ENGINE] [Account: {}] Routing calculation to calculate (Full-Path). ChangeFlags: {}, HasRelation: {}", 
                        accountId, changeFlags, task.isHasRelation());
                result = billingCalculator.calculate(config, consumptions, month, daysUsed);
            }

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
                ir.put("meter_point_id", u.getMaDdo());
                ir.put("calculation_type", getCalculationType(config, u.getMaDdo()));
                
                List<Map<String, Object>> subReadings = new ArrayList<>();
                Map<String, Object> sub = new HashMap<>();
                sub.put("seq", u.getLanDocPhu());
                sub.put("from_date", u.getTuNgay() != null ? u.getTuNgay().toString() : null);
                sub.put("to_date", u.getDenNgay() != null ? u.getDenNgay().toString() : null);
                sub.put("start_index", u.getChiSoDau());
                sub.put("end_index", u.getChiSoCuoi());
                sub.put("is_rollover", u.getCoQuayVong());
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
            taxCalc.put("vat_rate", vatRate);
            taxCalc.put("tax_amount_raw", taxAmount);
            taxCalc.put("rounding_mode", "HALF_UP");
            taxCalc.put("tax_amount_final", taxAmount);
            manifest.put("tax_calculation", taxCalc);
            
            manifest.put("total_final_amount", totalAfterTax);

            String manifestJson = objectMapper.writeValueAsString(manifest);
            String invoiceId = "INV-" + accountId + "-" + month + "-v" + version;
            String idempotencyKey = accountId + "_" + month + "_p" + task.getKyChot() + "_v" + version;

            String maDviqly = config.getMaDviqly() != null ? config.getMaDviqly() : "PD0600";

            boolean isProrated = proRataFactor.compareTo(BigDecimal.ONE) < 0;

                java.sql.Timestamp nowTs = java.sql.Timestamp.valueOf(LocalDateTime.now());
                billingStateRepository.upsertInvoice(
                    invoiceId,
                    accountId,
                    dtuongQly,
                    month,
                    totalBeforeTax,
                    taxAmount,
                    totalAfterTax,
                    idempotencyKey,
                    manifestJson,
                    isProrated,
                    accountId + "_" + month + "_v" + version,
                    "FINAL",
                    maDviqly,
                    nowTs,
                    nowTs
                );

            // [LOCKED-RULE] Khóa cứng Snapshot cước khi hóa đơn được phát hành
                billingStateRepository.lockSnapshot(accountId, month, task.getKyChot(), version);

            log.info("[AUDIT-TRACER] [Account: {}] Step 6: UPSERT transaction completed. Invoice saved to 'hoa_don' and Snapshot locked.", accountId);

            // 7. Save Outbox Event (Transactional Outbox Pattern)
            Map<String, Object> outboxPayload = new HashMap<>();
            outboxPayload.put("invoiceId", invoiceId);
            outboxPayload.put("accountId", accountId);
            outboxPayload.put("billingCycleMonth", month);
            outboxPayload.put("amountBeforeTax", totalBeforeTax);
            outboxPayload.put("taxAmount", taxAmount);
            outboxPayload.put("amountAfterTax", totalAfterTax);
            outboxPayload.put("timestamp", LocalDateTime.now().toString());

            billingStateRepository.insertOutboxEvent(
                    UUID.randomUUID(),
                    "INVOICE",
                    invoiceId,
                    "INVOICE_CREATED",
                    objectMapper.writeValueAsString(outboxPayload),
                    java.sql.Timestamp.valueOf(LocalDateTime.now())
            );

            log.info("[AUDIT-TRACER] [Account: {}] Step 6.1: Outbox event 'INVOICE_CREATED' saved.", accountId);

            // Update calculations tracking statuses
            long duration = System.currentTimeMillis() - tStart;
            if (meterRegistry != null) {
                meterRegistry.counter("billing.calculations", "status", "success").increment();
                meterRegistry.timer("billing.execution.time").record(duration, java.util.concurrent.TimeUnit.MILLISECONDS);
            }
            String targetStatus = "SUCCESS";
            if ("BATCH".equals(task.getTriggeredBy()) || "CMIS".equals(task.getTriggeredBy())) {
                targetStatus = "SUCCESS_CMIS";
            }
            boolean statusChanged = updateAccountStatus(dtuongQly, accountId, month, task.getKyChot(), targetStatus, invoiceId, null, duration, workerNodeId);
            if (statusChanged) {
                updateBookBillingRunProgress(dtuongQly, month, task.getKyChot(), 1, 1, 0);
            }

            // Enqueue log
            Map<String, Object> inputLogMap = new HashMap<>();
            inputLogMap.put("config", config);
            inputLogMap.put("consumptions", consumptions);
            String inputJson = objectMapper.writeValueAsString(inputLogMap);
            billingLogService.enqueueLog(dtuongQly, accountId, month, task.getKyChot(), targetStatus, inputJson, manifestJson, null);
        } catch (Exception e) {
            long duration = System.currentTimeMillis() - tStart;
            if (meterRegistry != null) {
                meterRegistry.counter("billing.calculations", "status", "error").increment();
                meterRegistry.timer("billing.execution.time").record(duration, java.util.concurrent.TimeUnit.MILLISECONDS);
            }
            boolean statusChanged = updateAccountStatus(dtuongQly, accountId, month, task.getKyChot(), "FAILED", null, e.getMessage(), duration, workerNodeId);
            if (statusChanged) {
                updateBookBillingRunProgress(dtuongQly, month, task.getKyChot(), 1, 0, 1);
            }
            billingLogService.enqueueLog(dtuongQly, accountId, month, task.getKyChot(), "FAILED", null, null, e.getMessage());
            throw e;
        }
    }

    @Transactional
    public void processBillingBatch(List<BillingTaskDto> tasks) throws Exception {
        if (tasks == null || tasks.isEmpty()) return;

        String firstDtuongQly = tasks.get(0).getDtuongQly() != null ? tasks.get(0).getDtuongQly() : "DEMAND";
        String firstMonth = tasks.get(0).getThangChuKy();
        
        warmupBookCache(firstDtuongQly, firstMonth, tasks.get(0).getKyChot());

        List<Object[]> invoiceBatch = new ArrayList<>();
        List<Object[]> outboxBatch = new ArrayList<>();
        List<Object[]> statusBatch = new ArrayList<>();

        for (BillingTaskDto task : tasks) {
            String accountId = task.getMaKhang();
            String month = task.getThangChuKy();
            int version = task.getPhienBanTinh();
            String dtuongQly = task.getDtuongQly() != null ? task.getDtuongQly() : "DEMAND";

            if (!tryClaimProcessingWorker(accountId, month, task.getKyChot())) {
                log.info("[SKIP-CALC-BATCH] Account {} is already being processed/finalized for month {} period {}.", accountId, month, task.getKyChot());
                continue;
            }

            try {
                // 1. Get validated usages from Kafka task DTO readings
                List<MeterUsage> usages = new ArrayList<>();
                if (task.getDanhSachChiSo() != null && !task.getDanhSachChiSo().isEmpty()) {
                    for (MeterReadingDto r : task.getDanhSachChiSo()) {
                        MeterUsage u = new MeterUsage();
                        u.setMaDdo(r.getMaDdo());
                        u.setTuNgay(r.getTuNgay());
                        u.setDenNgay(r.getDenNgay());
                        u.setChiSoDau(r.getChiSoDau());
                        u.setChiSoCuoi(r.getChiSoCuoi());
                        u.setConsumption(r.getSanLuong());
                        u.setMaKhang(accountId);
                        u.setThangChuKy(month);
                        u.setTrangThaiXuLy("VALIDATED");
                        u.setCoQuayVong(r.getCoQuayVong() != null ? r.getCoQuayVong() : false);
                        u.setMaxRegisterSnapshot(r.getMaxRegisterSnapshot());
                        u.setLanDocPhu(r.getLanDocPhu() != null ? r.getLanDocPhu() : 1);
                        u.setLoaiGhiIndex(r.getLoaiGhiIndex() != null ? r.getLoaiGhiIndex() : "ORIGINAL");
                        u.setTgianBdien(r.getTgianBdien() != null ? r.getTgianBdien() : "KT");
                        usages.add(u);
                    }
                } else {
                    usages = meterUsageRepository.findByMaKhangAndThangChuKyAndKyChotAndTrangThaiXuLy(accountId, month, task.getKyChot(), "VALIDATED");
                }
                if (usages.isEmpty()) {
                    throw new NoSuchElementException("No validated meter usage found/provided for account: " + accountId);
                }

                // Determine actual billing period length (daysUsed)
                LocalDateTime minFrom = null;
                LocalDateTime maxTo = null;
                for (MeterUsage u : usages) {
                    if (u.getTuNgay() != null) {
                        if (minFrom == null || u.getTuNgay().isBefore(minFrom)) {
                            minFrom = u.getTuNgay();
                        }
                    }
                    if (u.getDenNgay() != null) {
                        if (maxTo == null || u.getDenNgay().isAfter(maxTo)) {
                            maxTo = u.getDenNgay();
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
                String cacheKey = "snapshot:" + accountId + ":" + month + ":" + task.getKyChot();
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
                            .findByMaKhangAndThangChuKyAndKyChotAndPhienBanTinh(accountId, month, task.getKyChot(), version);
                    if (snapshotOpt.isEmpty()) {
                        // Fallback to version 1 if specific version not found
                        snapshotOpt = snapshotRepository
                                .findByMaKhangAndThangChuKyAndKyChotAndPhienBanTinh(accountId, month, task.getKyChot(), 1);
                    }
                    if (snapshotOpt.isEmpty()) {
                        throw new NoSuchElementException("No snapshot profile found for account: " + accountId + ", version: " + version);
                    }
                    config = snapshotOpt.get().getDuLieuCauHinh();
                    try {
                        redisTemplate.opsForValue().set(cacheKey, config, 24, TimeUnit.HOURS);
                    } catch (Exception e) {
                        // Ignore
                    }
                }

                // [II.2] Validate Snapshot completeness
                validateSnapshot(config, accountId);

                // 3. Collect node consumptions (multi-BCS aware: aggregate active power, exclude VC)
                Map<String, BigDecimal> consumptions = new HashMap<>();
                for (MeterUsage u : usages) {
                    BigDecimal cons = u.getConsumption() != null ? u.getConsumption() 
                            : u.getChiSoCuoi().subtract(u.getChiSoDau());
                    String tp = u.getTgianBdien() != null ? u.getTgianBdien() : "KT";

                    // Detail key per BCS register: METER-001_BT, METER-001_CD, METER-001_TD
                    String detailKey = u.getMaDdo() + "_" + tp;
                    consumptions.merge(detailKey, cons, BigDecimal::add);

                    // Aggregate active power per meterPoint (exclude reactive power VC)
                    if (!"VC".equals(tp)) {
                        consumptions.merge(u.getMaDdo(), cons, BigDecimal::add);
                    }
                }

                // Dynamics fetch VAT tax rate realtime from repository
                BigDecimal vatRate = BigDecimal.valueOf(0.08);
                try {
                    Optional<CauHinhThue> thueOpt = cauHinhThueRepository.findByLoaiThue("VAT");
                    if (thueOpt.isPresent()) {
                        vatRate = thueOpt.get().getThueSuat();
                    }
                } catch (Exception e) {
                    log.warn("[BILLING-WORKER-BATCH] Failed to fetch tax rate, using fallback default 0.08 (8%): {}", e.getMessage());
                }

                if (config.getSchemaSteps() != null) {
                    for (BillingSchemaStep step : config.getSchemaSteps()) {
                        if ("TAX".equals(step.getVariantName())) {
                            if (step.getStepConfig() == null) {
                                step.setStepConfig(new HashMap<>());
                            }
                            step.getStepConfig().put("taxRate", vatRate.doubleValue());
                        }
                    }
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
                    ir.put("meter_point_id", u.getMaDdo());
                    ir.put("calculation_type", getCalculationType(config, u.getMaDdo()));
                    
                    List<Map<String, Object>> subReadings = new ArrayList<>();
                    Map<String, Object> sub = new HashMap<>();
                    sub.put("seq", u.getLanDocPhu());
                    sub.put("from_date", u.getTuNgay() != null ? u.getTuNgay().toString() : null);
                    sub.put("to_date", u.getDenNgay() != null ? u.getDenNgay().toString() : null);
                    sub.put("start_index", u.getChiSoDau());
                    sub.put("end_index", u.getChiSoCuoi());
                    sub.put("is_rollover", u.getCoQuayVong());
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
                taxCalc.put("vat_rate", vatRate);
                taxCalc.put("tax_amount_raw", taxAmount);
                taxCalc.put("rounding_mode", "HALF_UP");
                taxCalc.put("tax_amount_final", taxAmount);
                manifest.put("tax_calculation", taxCalc);
                
                manifest.put("total_final_amount", totalAfterTax);

                String manifestJson = objectMapper.writeValueAsString(manifest);
                String idempotencyKey = accountId + "_" + month + "_p" + task.getKyChot() + "_v" + version;
                boolean isProrated = (BigDecimal.valueOf(daysUsed).compareTo(BigDecimal.valueOf(daysInMonth)) < 0) && daysUsed > 0;

                String maDviqly = config.getMaDviqly() != null ? config.getMaDviqly() : "PD0600";

                // Add to invoice batch params
                invoiceBatch.add(new Object[] {
                    invoiceId,
                    accountId,
                    dtuongQly,
                    month,
                    task.getKyChot(),
                    totalBeforeTax,
                    taxAmount,
                    totalAfterTax,
                    idempotencyKey,
                    manifestJson,
                    isProrated,
                    accountId + "_" + month + "_p" + task.getKyChot() + "_v" + version,
                    "FINAL",
                    maDviqly,
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

                String targetStatus = "SUCCESS";
                if ("BATCH".equals(task.getTriggeredBy()) || "CMIS".equals(task.getTriggeredBy())) {
                    targetStatus = "SUCCESS_CMIS";
                }
                if (totalBeforeTax != null && totalBeforeTax.compareTo(java.math.BigDecimal.valueOf(anomalyThresholdVnd)) > 0) {
                    targetStatus = "ANOMALY";
                }
                // Status success batch
                statusBatch.add(new Object[] {
                    accountId,
                    month,
                    dtuongQly,
                    task.getKyChot(),
                    targetStatus,
                    invoiceId,
                    null,
                    workerNodeId,
                    java.sql.Timestamp.valueOf(LocalDateTime.now())
                });

                // Enqueue success log
                Map<String, Object> inputLogMap = new HashMap<>();
                inputLogMap.put("config", config);
                inputLogMap.put("consumptions", consumptions);
                String inputJson = objectMapper.writeValueAsString(inputLogMap);
                billingLogService.enqueueLog(dtuongQly, accountId, month, task.getKyChot(), targetStatus, inputJson, manifestJson, null);

            } catch (Exception e) {
                log.error("Calculation failed for account: {}, error: {}", accountId, e.getMessage(), e);
                // Status fail batch
                statusBatch.add(new Object[] {
                    accountId,
                    month,
                    dtuongQly,
                    task.getKyChot(),
                    "FAILED",
                    null,
                    e.getMessage(),
                    workerNodeId,
                    java.sql.Timestamp.valueOf(LocalDateTime.now())
                });
                // Enqueue failed log
                billingLogService.enqueueLog(dtuongQly, accountId, month, task.getKyChot(), "FAILED", null, null, e.getMessage());
            }
        }

        // 6. Execute atomic Batch UPSERT on Citus/TiDB
        if (!invoiceBatch.isEmpty()) {
            billingStateRepository.batchUpsertInvoices(invoiceBatch);
            billingStateRepository.batchInsertOutbox(outboxBatch);
            
            log.info("[AUDIT-TRACER] Batch transaction committed. Saved {} invoices & outbox events to Postgres.", invoiceBatch.size());
        }

        // 7. Write run states
        if (!statusBatch.isEmpty()) {
            billingStateRepository.batchUpsertStatuses(statusBatch);

            int firstPeriod = tasks.get(0).getKyChot();
            String hashKey = "billing:book_status_hash:" + firstDtuongQly + ":" + firstMonth + ":" + firstPeriod;
            Map<String, String> localMap = localBookStatusCache.get(firstDtuongQly + ":" + firstMonth + ":" + firstPeriod);
            Map<String, String> redisUpdates = new HashMap<>();
            
            int processedDelta = 0;
            int successDelta = 0;
            int failedDelta = 0;

            for (Object[] row : statusBatch) {
                String accId = (String) row[0];
                String stat = (String) row[4];

                if (localMap != null) {
                    localMap.put(accId, stat);
                }
                redisUpdates.put(accId, stat);

                processedDelta++;
                if (Arrays.asList("SUCCESS", "SUCCESS_CMIS", "ANOMALY", "LOCKED", "E_INVOICE_ISSUED").contains(stat)) {
                    successDelta++;
                } else if ("FAILED".equals(stat)) {
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

            updateBookBillingRunProgress(firstDtuongQly, firstMonth, tasks.get(0).getKyChot(), processedDelta, successDelta, failedDelta);
            log.info("[AUDIT-TRACER] Persistent Billing Status written for {} accounts. Success: {}, Failed: {}.", processedDelta, successDelta, failedDelta);
            checkAndTriggerAutoBatch(firstDtuongQly, firstMonth, tasks.get(0).getKyChot());
        }
    }

    private void validateSnapshot(BillingConfigSnapshot config, String accountId) {
        if (config == null) {
            throw new com.evn.billing.worker.exception.MalformSnapshotException("Snapshot config is null for account: " + accountId);
        }
        if (config.getMaKhang() == null || config.getMaKhang().isEmpty()) {
            throw new com.evn.billing.worker.exception.MalformSnapshotException("Missing accountId in snapshot config for account: " + accountId);
        }
        if (config.getDtuongQly() == null || config.getDtuongQly().isEmpty()) {
            throw new com.evn.billing.worker.exception.MalformSnapshotException("Missing dtuongQly in snapshot config for account: " + accountId);
        }
        if (config.getNgayHieuLuc() == null) {
            throw new com.evn.billing.worker.exception.MalformSnapshotException("Missing effectiveSyncDate in snapshot config for account: " + accountId);
        }
        if (config.getMeterTopology() == null || config.getMeterTopology().getRootPoints() == null || config.getMeterTopology().getRootPoints().isEmpty()) {
            throw new com.evn.billing.worker.exception.MalformSnapshotException("Missing or empty meterTopology in snapshot config for account: " + accountId);
        }
        if (config.getBieuGia() == null || config.getBieuGia().isEmpty()) {
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
        if (meterPointId.equals(node.getMaDdo())) {
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

    private List<String> getParentAccountIds(String childAccountId) {
        return billingStateRepository.findParentAccountIds(childAccountId);
    }

    @Transactional
    public void lockBilling(String accountId, String month, int period, String targetStatus) throws Exception {
        Map<String, Object> row;
        try {
            row = billingStateRepository.findStatusRowForUpdate(accountId, month, period);
        } catch (Exception e) {
            throw new NoSuchElementException("Không tìm thấy thông tin cước cho khách hàng: " + accountId + ", kỳ: " + month + ", đợt: " + period);
        }

        String current = (String) row.get("trang_thai");
        String dtuongQly = (String) row.get("dtuong_qly");

        if (targetStatus.equals(current)) {
            log.info("[LOCK-BILL] Account {} already in target status {} for kỳ: {}, đợt: {}", accountId, targetStatus, month, period);
            return;
        }

        boolean allowed;
        if ("LOCKED".equals(targetStatus)) {
            allowed = Arrays.asList("SUCCESS", "SUCCESS_CMIS", "ANOMALY").contains(current);
        } else if ("SUCCESS_CMIS".equals(targetStatus)) {
            allowed = Arrays.asList("SUCCESS", "ANOMALY").contains(current);
        } else if ("E_INVOICE_ISSUED".equals(targetStatus)) {
            allowed = Arrays.asList("LOCKED", "SUCCESS_CMIS").contains(current);
        } else {
            allowed = !Arrays.asList("CANCELLED", "FAILED", "PENDING", "PROCESSING").contains(current);
        }

        if (!allowed) {
            throw new IllegalStateException("Không thể chuyển trạng thái từ " + current + " sang " + targetStatus + " cho khách hàng " + accountId);
        }

        billingStateRepository.updateAccountStatus(targetStatus, accountId, month, period);

        // Update Redis Cache
        String hashKey = "billing:book_status_hash:" + dtuongQly + ":" + month + ":" + period;
        try {
            redisTemplate.opsForHash().put(hashKey, accountId, targetStatus);
        } catch (Exception e) {
            log.warn("[LOCK-BILL] Failed to update Redis status to {}: {}", targetStatus, e.getMessage());
        }

        // Update local JVM cache
        String localKey = dtuongQly + ":" + month + ":" + period;
        Map<String, String> localMap = localBookStatusCache.get(localKey);
        if (localMap != null) {
            localMap.put(accountId, targetStatus);
        }
        log.info("[LOCK-BILL] Locked status of Account: {} to {} for kỳ: {}, đợt: {}", accountId, targetStatus, month, period);
    }

    @Transactional
    public void cancelBilling(String accountId, String month, int period) throws Exception {
        Map<String, Object> row;
        try {
            row = billingStateRepository.findStatusRowForUpdate(accountId, month, period);
        } catch (Exception e) {
            throw new NoSuchElementException("Không tìm thấy thông tin cước đã tính cho khách hàng: " + accountId + ", kỳ: " + month + ", đợt: " + period);
        }

        String dtuongQly = (String) row.get("dtuong_qly");
        String oldStatus = (String) row.get("trang_thai");

        if ("CANCELLED".equals(oldStatus)) {
            log.info("[CANCEL-BILL] Account {} already CANCELLED for kỳ: {}, đợt: {}", accountId, month, period);
            return;
        }
        
        // Gated Pipeline Lock Rule: Cấm hủy cước nếu hóa đơn đã được phát hành HĐĐT hoặc đã khóa
        if ("LOCKED".equals(oldStatus) || "E_INVOICE_ISSUED".equals(oldStatus)) {
            throw new IllegalStateException("Hóa đơn của khách hàng " + accountId + " kỳ " + month + " đợt " + period + 
                    " đã được phát hành hóa đơn điện tử hoặc đã khóa. Không thể thực hiện hủy cước trực tiếp!");
        }

        log.info("[CANCEL-BILL] Cancelling billing for Account: {}, Month: {}, Period: {}, Book: {}, Old Status: {}", 
                accountId, month, period, dtuongQly, oldStatus);

        // Append-Only Rule: Mark invoices as CANCELLED instead of deleting
        billingStateRepository.markInvoicesCancelled(accountId, month, period);
        log.info("[CANCEL-BILL] Marked invoices as CANCELLED in 'hoa_don' table.");

        // Mở khóa snapshot liên quan về DRAFT
        billingStateRepository.setSnapshotsDraft(accountId, month, period);
        log.info("[CANCEL-BILL] Reset snapshot status to DRAFT to allow CMIS updates.");

        // Keep nhat_ky_tinh_toan for auditing trail

        billingStateRepository.markAccountCancelled(accountId, month, period, "Hủy hóa đơn bởi người vận hành");

        String hashKey = "billing:book_status_hash:" + dtuongQly + ":" + month + ":" + period;
        try {
            redisTemplate.opsForHash().put(hashKey, accountId, "CANCELLED");
        } catch (Exception e) {
            log.warn("[CANCEL-BILL] Failed to update Redis status to CANCELLED: {}", e.getMessage());
        }

        String localKey = dtuongQly + ":" + month + ":" + period;
        Map<String, String> localMap = localBookStatusCache.get(localKey);
        if (localMap != null) {
            localMap.put(accountId, "CANCELLED");
        }

        if ("SUCCESS".equals(oldStatus) || "SUCCESS_CMIS".equals(oldStatus) || "ANOMALY".equals(oldStatus)) {
            updateBookBillingRunProgress(dtuongQly, month, period, -1, -1, 0);
        } else if ("FAILED".equals(oldStatus)) {
            updateBookBillingRunProgress(dtuongQly, month, period, -1, 0, -1);
        }
        log.info("[CANCEL-BILL] Billing run progress decremented.");

        // Cascading Cancellation: Recursively cancel all parent accounts that depend on this child account
        List<String> parentAccountIds = getParentAccountIds(accountId);
        for (String parentId : parentAccountIds) {
            log.info("[CASCADING-CANCEL] Parent account '{}' depends on canceled child '{}'. Triggering cascading cancellation on parent...", parentId, accountId);
            try {
                cancelBilling(parentId, month, period);
            } catch (NoSuchElementException e) {
                // If parent has not been calculated yet (status is not SUCCESS/FAILED), ignore
                log.info("[CASCADING-CANCEL] Parent account '{}' has not been calculated yet. Skipping cancel command.", parentId);
            }
        }
    }

    public Map<String, Object> getBookProgress(String dtuongQly, String month, int period) {
        Optional<DtuongQlySchedule> scheduleOpt = dtuongQlyScheduleRepository
                 .findByDtuongQlyAndThangCkAndKyChot(dtuongQly, month, period);
        
        Map<String, Object> result = new HashMap<>();
        if (scheduleOpt.isEmpty()) {
            result.put("dtuongQly", dtuongQly);
            result.put("billingCycleMonth", month);
            result.put("period", period);
            result.put("totalAccounts", 0);
            result.put("processedAccounts", 0);
            result.put("successAccounts", 0);
            result.put("failedAccounts", 0);
            result.put("readingsValidated", 0);
            result.put("readingsPending", 0);
            return result;
        }

        DtuongQlySchedule schedule = scheduleOpt.get();
        result.put("dtuongQly", schedule.getDtuongQly());
        result.put("billingCycleMonth", schedule.getThangCk());
        result.put("period", schedule.getKyChot());
        result.put("totalAccounts", schedule.getTongKh());
        result.put("processedAccounts", schedule.getKhDaXl());
        result.put("successAccounts", schedule.getKhTc());
        result.put("failedAccounts", schedule.getKhTb());

        int validated = billingStateRepository.countValidatedReadings(dtuongQly, month, period);
        result.put("readingsValidated", validated);
        result.put("readingsPending", Math.max(0, schedule.getTongKh() - validated));

        // Detailed CMIS Station metrics
        int pending = billingStateRepository.countByStatuses(dtuongQly, month, period, Arrays.asList("PENDING", "CANCELLED", "FAILED"));
        int anomaly = billingStateRepository.countByStatuses(dtuongQly, month, period, List.of("ANOMALY"));
        int success = billingStateRepository.countByStatuses(dtuongQly, month, period, Arrays.asList("SUCCESS", "SUCCESS_CMIS", "LOCKED"));
        int issued = billingStateRepository.countByStatuses(dtuongQly, month, period, List.of("E_INVOICE_ISSUED"));
        int incomplete = billingStateRepository.countByStatuses(dtuongQly, month, period, List.of("INCOMPLETE"));
        int pendingManual = billingStateRepository.countByStatuses(dtuongQly, month, period, List.of("PENDING_MANUAL"));
        int failed = billingStateRepository.countByStatuses(dtuongQly, month, period, List.of("FAILED"));
        int cancelled = billingStateRepository.countByStatuses(dtuongQly, month, period, List.of("CANCELLED"));
        int processing = billingStateRepository.countByStatuses(dtuongQly, month, period, List.of("PROCESSING"));
        int suspect = billingStateRepository.countByStatuses(dtuongQly, month, period, List.of("SUSPECT"));

        result.put("pendingReadings", pending);
        result.put("anomalousInvoices", anomaly);
        result.put("successfulInvoices", success);
        result.put("issuedInvoices", issued);
        result.put("incompleteReadings", incomplete);
        result.put("pendingManualReview", pendingManual);
        result.put("failedCalculation", failed);
        result.put("cancelledBilling", cancelled);
        result.put("processingBilling", processing);
        result.put("suspectReadings", suspect);

        return result;
    }
 
    public org.springframework.data.domain.Page<AccountBillingStatus> getAccountsByStatus(
            String dtuongQly, String month, int period, List<String> statuses, org.springframework.data.domain.Pageable pageable) {
        return accountBillingStatusRepository.findByDtuongQlyAndThangChuKyAndKyChotAndTrangThaiIn(dtuongQly, month, period, statuses, pageable);
    }

    @Transactional
    public void lockBookBilling(String dtuongQly, String month, int period, String targetStatus) throws Exception {
        log.info("[LOCK-BOOK-BILL] Request to lock all calculated billing for Book: {}, Month: {}, Period: {}, Target Status: {}", 
                dtuongQly, month, period, targetStatus);

        List<String> accountIds = billingStateRepository.findLockableAccountsForBook(dtuongQly, month, period);
        if (accountIds.isEmpty()) {
            log.info("[LOCK-BOOK-BILL] No calculated billing records to lock for Book: {}, Month: {}, Period: {}", dtuongQly, month, period);
            return;
        }

        billingStateRepository.lockBookAccounts(dtuongQly, month, period, targetStatus);

        String hashKey = "billing:book_status_hash:" + dtuongQly + ":" + month + ":" + period;
        Map<String, String> redisUpdates = new HashMap<>();
        String localKey = dtuongQly + ":" + month + ":" + period;
        Map<String, String> localMap = localBookStatusCache.get(localKey);

        for (String accId : accountIds) {
            redisUpdates.put(accId, targetStatus);
            if (localMap != null) {
                localMap.put(accId, targetStatus);
            }
        }

        try {
            if (!redisUpdates.isEmpty()) {
                redisTemplate.opsForHash().putAll(hashKey, redisUpdates);
            }
        } catch (Exception e) {
            log.warn("[LOCK-BOOK-BILL] Failed to update Redis status for book: {}", e.getMessage());
        }

        log.info("[LOCK-BOOK-BILL] Successfully locked {} accounts of Book: {} to {} for kỳ: {}, đợt: {}", 
            accountIds.size(), dtuongQly, targetStatus, month, period);
    }
 
    private void checkAndTriggerAutoBatch(String dtuongQly, String month, int period) {
        try {
            // Count success + anomaly vs total accounts in this book
            Integer total = billingStateRepository.countTotalAccounts(dtuongQly, month, period);
            if (total == null || total == 0) return;
 
            Integer success = billingStateRepository.countSuccessfulForAutoBatch(dtuongQly, month, period);
            if (success == null) success = 0;
 
            double ratio = (success * 100.0) / total;
            
            // Read threshold from config (default 95)
            int threshold = 95;
            try {
                Integer configThreshold = billingStateRepository.findAutoBatchThreshold();
                if (configThreshold != null) {
                    threshold = configThreshold;
                }
            } catch (Exception ex) {
                // ignore, use default 95
            }
 
            if (ratio >= threshold) {
                // Check if book run status is not already COMPLETED or PROCESSING
                String tthaiChay = billingStateRepository.findBookRunStatus(dtuongQly, month, period);
 
                if (!"COMPLETED".equalsIgnoreCase(tthaiChay) && !"PROCESSING".equalsIgnoreCase(tthaiChay)) {
                    log.info("[AUTO-BATCH] Book: {} success ratio: {}% exceeds threshold {}%. Auto-triggering Batch Job via Kafka.",
                            dtuongQly, ratio, threshold);
                    
                    Map<String, Object> autoBatchEvent = new HashMap<>();
                    autoBatchEvent.put("dtuongQly", dtuongQly);
                    autoBatchEvent.put("month", month);
                    autoBatchEvent.put("period", period);
                    autoBatchEvent.put("timestamp", LocalDateTime.now().toString());
 
                    // Publish to Kafka billing-auto-batch-topic
                    kafkaTemplate.send("billing-auto-batch-topic", dtuongQly, objectMapper.writeValueAsString(autoBatchEvent));
                }
            }
        } catch (Exception e) {
            log.error("[AUTO-BATCH] Error checking auto batch trigger for book: {}", dtuongQly, e);
        }
    }
 
    @Transactional
    public void approveBookBilling(String dtuongQly, String month, int period, List<String> excludedAccounts) throws Exception {
        log.info("[APPROVE-BOOK] CMIS Approve Book Billing for Book: {}, Month: {}, Period: {}, Excluded: {}",
                dtuongQly, month, period, excludedAccounts);
 
        List<String> excluded = excludedAccounts != null ? excludedAccounts : new ArrayList<>();
 
        if (excluded.isEmpty()) {
            billingStateRepository.approveBookAll(dtuongQly, month, period);
        } else {
            billingStateRepository.approveBookExcluding(dtuongQly, month, period, excluded);
        }
 
        for (String accId : excluded) {
            try {
                billingStateRepository.rejectAccountByCmis(accId, month, period, "CMIS Rejected this account during book approval.");
                cancelBilling(accId, month, period);
                String hashKey = "billing:book_status_hash:" + dtuongQly + ":" + month + ":" + period;
                redisTemplate.opsForHash().put(hashKey, accId, "REJECTED_CMIS");
            } catch (Exception ex) {
                log.error("[APPROVE-BOOK] Failed to reject/cancel billing for account: {}", accId, ex);
            }
        }
 
        String hashKey = "billing:book_status_hash:" + dtuongQly + ":" + month + ":" + period;
        List<String> approvedAccounts = billingStateRepository.findApprovedAccounts(dtuongQly, month, period);
        Map<String, String> redisUpdates = new HashMap<>();
        for (String accId : approvedAccounts) {
            redisUpdates.put(accId, "APPROVED_CMIS");
        }
        if (!redisUpdates.isEmpty()) {
            redisTemplate.opsForHash().putAll(hashKey, redisUpdates);
        }
    }
}
