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
import com.evn.billing.common.domain.BillInvoice;
import com.evn.billing.common.dto.MeterDetails;
import com.evn.billing.common.dto.PriceApplicationRule;
import java.time.LocalDate;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.web.client.RestTemplate;
import com.evn.billing.common.dto.GenerateAccountSnapshotRequest;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.sql.Timestamp;
import java.time.temporal.ChronoUnit;
import java.time.YearMonth;
import java.util.stream.Collectors;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.HttpEntity;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

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
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private CauHinhThueRepository cauHinhThueRepository;

    @Autowired(required = false)
    private MeterRegistry meterRegistry;

    private final BillingCalculator billingCalculator = new BillingCalculator();
 
    @Value("${billing.worker.claim-timeout-minutes:15}")
    private int claimTimeoutMinutes;
 
    @Value("${billing.worker.anomaly-threshold-vnd:1000000}")
    private double anomalyThresholdVnd;

    @Autowired
    private SelfHealingService selfHealingService;

    @Autowired
    private CancelBillingService cancelBillingService;


 
    private final Map<String, Map<String, String>> localBookStatusCache = new ConcurrentHashMap<>();

    private final String workerNodeId = initWorkerNodeId();

    // FIX-03: VAT rate cache — 1 lần/giờ thay vì 1 lần/KH
    private volatile BigDecimal cachedVatRate = null;
    private volatile long vatRateCachedAt = 0L;
    private static final long VAT_CACHE_TTL_MS = 3_600_000L; // 1 giờ

    private String initWorkerNodeId() {
        try {
            return java.net.InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "worker-" + UUID.randomUUID();
        }
    }

    /**
     * FIX-03: Lấy VAT rate với local cache TTL 1 giờ.
     * Tránh 1000 SELECT/lô cho cùng 1 giá trị không thay đổi.
     */
    private BigDecimal fetchVatRateWithCache() {
        long now = System.currentTimeMillis();
        if (cachedVatRate != null && (now - vatRateCachedAt) < VAT_CACHE_TTL_MS) {
            return cachedVatRate;
        }
        try {
            Optional<CauHinhThue> thueOpt = cauHinhThueRepository.findByLoaiThue("VAT");
            if (thueOpt.isPresent()) {
                cachedVatRate = thueOpt.get().getThueSuat();
                vatRateCachedAt = now;
                log.info("[VAT-CACHE] Refreshed VAT rate from DB: {}", cachedVatRate);
                return cachedVatRate;
            }
        } catch (Exception e) {
            log.warn("[VAT-CACHE] Failed to fetch VAT rate from DB: {}. Using default 8%.", e.getMessage());
        }
        // Fallback không dùng default tĩnh — throw nếu cần strict mode
        // Hiện tại dùng 0.08 vì đây là giá trị hệ thống đã được cấu hình
        return BigDecimal.valueOf(0.08);
    }

    private boolean tryClaimProcessingWorker(String maKhang, String month, int period) {
        int updated = billingStateRepository.tryClaimProcessingWorker(workerNodeId, maKhang, month, period, claimTimeoutMinutes);
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

        Map<String, String> localMap = new ConcurrentHashMap<>();
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

    public String getAccountStatus(String dtuongQly, String maKhang, String month, int period) {
        if (dtuongQly == null || dtuongQly.isEmpty()) return null;
        String localKey = dtuongQly + ":" + month + ":" + period;
        Map<String, String> localMap = localBookStatusCache.get(localKey);
        if (localMap != null && localMap.containsKey(maKhang)) {
            return localMap.get(maKhang);
        }

        String hashKey = "billing:book_status_hash:" + dtuongQly + ":" + month + ":" + period;
        try {
            Object val = redisTemplate.opsForHash().get(hashKey, maKhang);
            if (val != null) {
                return val.toString();
            }
        } catch (Exception e) {
            // Ignore
        }

        Optional<AccountBillingStatus> dbStatus = accountBillingStatusRepository
                .findById(new AccountBillingStatusId(maKhang, month, period));
        if (dbStatus.isPresent()) {
            String dbVal = dbStatus.get().getTrangThai();
            try {
                redisTemplate.opsForHash().put(hashKey, maKhang, dbVal);
            } catch (Exception e) {
                // Ignore
            }
            return dbVal;
        }

        return null;
    }

    public boolean updateAccountStatus(String dtuongQly, String maKhang, String month, int period, String status, String invoiceId, String errorMsg, Long durationMs, String workerNode) {
        if (dtuongQly == null || dtuongQly.isEmpty()) return false;
        billingStateRepository.seedProcessingStatus(maKhang, month, dtuongQly, period, workerNode);

        int affected = billingStateRepository.updateProcessingStatus(
                status,
                invoiceId,
                errorMsg,
                durationMs,
                workerNode,
                dtuongQly,
                maKhang,
                month,
                period
        );

        if (affected == 0) {
            log.warn("[STATUS-GUARD] Skip status update for Account: {}, Month: {}, Period: {} -> {} because row is no longer claimable by worker {}.",
                maKhang, month, period, status, workerNode);
            return false;
        }

        String hashKey = "billing:book_status_hash:" + dtuongQly + ":" + month + ":" + period;
        try {
            redisTemplate.opsForHash().put(hashKey, maKhang, status);
        } catch (Exception e) {
            // Ignore
        }

        String localKey = dtuongQly + ":" + month + ":" + period;
        Map<String, String> localMap = localBookStatusCache.get(localKey);
        if (localMap != null) {
            localMap.put(maKhang, status);
        }
        return true;
    }

    public void updateBookBillingRunProgress(String dtuongQly, String month, int period, int processedDelta, int successDelta, int failedDelta) {
        if (dtuongQly == null || dtuongQly.isEmpty()) return;
        billingStateRepository.updateBookBillingRunProgress(dtuongQly, month, period, processedDelta, successDelta, failedDelta);
    }

    @Transactional
    public void calculateImmediate(String maKhang, String month, Integer period, Integer version, String dtuongQly, String triggeredBy) throws Exception {
        if (maKhang == null || maKhang.trim().isEmpty()) {
            throw new IllegalArgumentException("Dữ liệu đầu vào thiếu thông tin mã khách hàng (maKhang) bắt buộc.");
        }
        if (month == null || month.trim().isEmpty()) {
            throw new IllegalArgumentException("Dữ liệu đầu vào thiếu thông tin tháng chu kỳ (month) bắt buộc.");
        }
        if (period == null) {
            throw new IllegalArgumentException("Dữ liệu đầu vào thiếu thông tin kỳ chốt (period) bắt buộc.");
        }
        if (version == null) {
            throw new IllegalArgumentException("Dữ liệu đầu vào thiếu thông tin phiên bản tính (version) bắt buộc.");
        }
        if (dtuongQly == null || dtuongQly.trim().isEmpty()) {
            throw new IllegalArgumentException("Dữ liệu đầu vào thiếu thông tin đối tượng quản lý (dtuongQly) bắt buộc.");
        }
        if (triggeredBy == null || triggeredBy.trim().isEmpty()) {
            throw new IllegalArgumentException("Dữ liệu đầu vào thiếu thông tin nguồn kích hoạt (triggeredBy) bắt buộc.");
        }

        // Seed processing status to ensure a record exists
        billingStateRepository.seedProcessingStatus(maKhang, month, dtuongQly, period, workerNodeId);

        // Reset status to PROCESSING and clear previous worker claims to ensure tryClaimProcessingWorker succeeds
        billingStateRepository.updateAccountStatus("PROCESSING", maKhang, month, period);

        // Evict status from Redis and local cache to avoid stale status reads
        String hashKey = "billing:book_status_hash:" + dtuongQly + ":" + month + ":" + period;
        try {
            redisTemplate.opsForHash().delete(hashKey, maKhang);
        } catch (Exception e) {
            // Ignore
        }
        String localKey = dtuongQly + ":" + month + ":" + period;
        Map<String, String> localMap = localBookStatusCache.get(localKey);
        if (localMap != null) {
            localMap.remove(maKhang);
        }

        BillingTaskDto task = new BillingTaskDto(maKhang, dtuongQly, month, period, version, "on_demand_trace");
        task.setTriggeredBy(triggeredBy);
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
        String maKhang = task.getMaKhang();
        String month = task.getThangChuKy();
        int version = task.getPhienBanTinh();
        String dtuongQly = task.getDtuongQly();

        // 0. Claim processing ownership to prevent duplicate execution from retry/rebalance.
        if (!tryClaimProcessingWorker(maKhang, month, task.getKyChot())) {
            log.info("[SKIP-CALC] Account {} is already being processed or finalized for month {} period {}.", maKhang, month, task.getKyChot());
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
                    u.setMaKhang(maKhang);
                    u.setThangChuKy(month);
                    u.setTrangThaiXuLy("VALIDATED");
                    u.setCoQuayVong(r.getCoQuayVong() != null ? r.getCoQuayVong() : false);
                    u.setMaxRegisterSnapshot(r.getMaxRegisterSnapshot());
                    
                    if (r.getLanDocPhu() == null) {
                        throw new IllegalArgumentException("Dữ liệu chỉ số thiếu thông tin lần đọc phụ (lanDocPhu) bắt buộc cho điểm đo: " + r.getMaDdo());
                    }
                    u.setLanDocPhu(r.getLanDocPhu());
                    u.setLoaiGhiIndex(r.getLoaiGhiIndex() != null ? r.getLoaiGhiIndex() : "ORIGINAL");
                    u.setTgianBdien(r.getTgianBdien() != null ? r.getTgianBdien() : "KT");
                    usages.add(u);
                }
            } else {
                // Fallback to database query if no readings provided in Kafka payload (e.g. REST fallback)
                usages = meterUsageRepository.findByMaKhangAndThangChuKyAndKyChotAndTrangThaiXuLy(maKhang, month, task.getKyChot(), "VALIDATED");
            }
            if (usages.isEmpty()) {
                throw new NoSuchElementException("No validated meter usage found/provided for account: " + maKhang);
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
            long daysUsed = ChronoUnit.DAYS.between(minFrom.toLocalDate(), maxTo.toLocalDate()) + 1;

            // Compute actual days of that billing cycle month
            int daysInMonth = 30;
            if (month != null && month.contains("_")) {
                try {
                    String[] parts = month.split("_");
                    int year = Integer.parseInt(parts[0]);
                    int monthVal = Integer.parseInt(parts[1]);
                    YearMonth yearMonth = YearMonth.of(year, monthVal);
                    daysInMonth = yearMonth.lengthOfMonth();
                } catch (Exception e) {
                    // Ignore
                }
            }

            // 2. Fetch Frozen Snapshot from Redis Cache (Cache-aside)
            String cacheKey = "snapshot:" + maKhang + ":" + month + ":" + task.getKyChot();
            BillingConfigSnapshot config = null;
            String changeFlags = task.getChangeFlags() != null ? task.getChangeFlags() : "NONE";
            
            boolean needsInvalidate = "PRICE_CHANGE".equals(changeFlags)
                    || "METER_CHANGE".equals(changeFlags)
                    || "MULTI_CHANGE".equals(changeFlags);

            if (!needsInvalidate && task.getSnapshotVersion() != null) {
                try {
                    Object cachedObj = redisTemplate.opsForValue().get(cacheKey);
                    if (cachedObj != null) {
                        BillingConfigSnapshot cached = (cachedObj instanceof BillingConfigSnapshot)
                                ? (BillingConfigSnapshot) cachedObj
                                : objectMapper.convertValue(cachedObj, BillingConfigSnapshot.class);
                        if (cached.getPhienBanTinh() != null && cached.getPhienBanTinh() < task.getSnapshotVersion()) {
                            needsInvalidate = true;
                            log.info("[SNAP-STALE] Cache version {} < task required version {}. Force invalidate for account: {}",
                                    cached.getPhienBanTinh(), task.getSnapshotVersion(), maKhang);
                        } else {
                            config = cached;
                        }
                    }
                } catch (Exception e) {
                    log.warn("Redis read failure, falling back: {}", e.getMessage());
                }
            } else if (!needsInvalidate) {
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
                    log.warn("Redis offline: {}", e.getMessage());
                }
            }

            if (needsInvalidate) {
                try {
                    redisTemplate.delete(cacheKey);
                    log.info("[SNAP-INVALIDATE] Invalidated snapshot cache for account: {}", maKhang);
                } catch (Exception e) {
                    log.warn("Failed to invalidate cache: {}", e.getMessage());
                }
            }

            if (config == null) {
                Optional<BillingAccountSnapshot> snapshotOpt = snapshotRepository
                        .findByMaKhangAndThangChuKyAndKyChotAndPhienBanTinh(maKhang, month, task.getKyChot(), version);
                if (snapshotOpt.isEmpty()) {
                    snapshotOpt = snapshotRepository
                            .findByMaKhangAndThangChuKyAndKyChotAndPhienBanTinh(maKhang, month, task.getKyChot(), 1);
                }
                if (snapshotOpt.isEmpty()) {
                    log.warn("[SNAP-FALLBACK] Sending Kafka recreate for account: {}", maKhang);
                    kafkaTemplate.send("snapshot-recreate-topic", maKhang, Map.of(
                        "loai", "ACCOUNT",
                        "ma_khang", maKhang,
                        "thang_chu_ky", month,
                        "ky_chot", task.getKyChot(),
                        "rule_id", "R-01",
                        "bang_nguon", "billing_worker",
                        "truong_thay_doi", "on_demand"
                    ));
                    
                    for (int retry = 0; retry < 3; retry++) {
                        try {
                            Thread.sleep(2000L * (retry + 1));
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                        }
                        snapshotOpt = snapshotRepository
                                .findByMaKhangAndThangChuKyAndKyChotAndPhienBanTinh(maKhang, month, task.getKyChot(), 1);
                        if (snapshotOpt.isPresent()) {
                            break;
                        }
                    }
                    
                    if (snapshotOpt.isEmpty()) {
                        throw new NoSuchElementException("No snapshot after on-demand Kafka trigger for account: " + maKhang);
                    }
                    log.info("[SNAP-FALLBACK] On-demand snapshot available for account: {}", maKhang);
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
                validateSnapshot(config, maKhang);
            } else {
                log.info("[ROUTING-ENGINE] Skip snapshot validation check for Fast-Path account: {}", maKhang);
            }

            log.info("[AUDIT-TRACER] [Account: {}] Step 4: Kafka calculation task received. Triggering billing engine processing.", maKhang);

            // 3. Collect node consumptions (multi-BCS aware: aggregate active power, exclude VC)
            Map<String, MeterUsage> latestUsagePerBcs = new LinkedHashMap<>();
            for (MeterUsage u : usages) {
                String tp = u.getTgianBdien() != null ? u.getTgianBdien() : "KT";
                String bcsKey = u.getMaDdo() + "|" + tp;
                latestUsagePerBcs.merge(bcsKey, u, (existing, incoming) -> {
                    int existingSeq = existing.getLanDocPhu() != null ? existing.getLanDocPhu() : 1;
                    int incomingSeq = incoming.getLanDocPhu() != null ? incoming.getLanDocPhu() : 1;
                    return incomingSeq > existingSeq ? incoming : existing;
                });
            }
            List<MeterUsage> deduplicatedUsages = new ArrayList<>(latestUsagePerBcs.values());

            Map<String, BigDecimal> consumptions = new HashMap<>();
            for (MeterUsage u : deduplicatedUsages) {
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
                    maKhang, config.getBieuGia().keySet(), daysInMonth, daysUsed, proRataFactor);

            // 4. Invoke Core Stateless Rating Engine
            CalculationResult result;
            if (isFastPath) {
                log.info("[ROUTING-ENGINE] [Account: {}] Routing calculation to calculateFastPath.", maKhang);
                result = billingCalculator.calculateFastPath(config, consumptions, month, daysUsed);
            } else {
                log.info("[ROUTING-ENGINE] [Account: {}] Routing calculation to calculate (Full-Path). ChangeFlags: {}, HasRelation: {}", 
                        maKhang, changeFlags, task.isHasRelation());
                result = billingCalculator.calculate(config, consumptions, month, daysUsed);
            }

            BigDecimal totalBeforeTax = result.getTotalAmountBeforeTax();
            BigDecimal taxAmount = result.getTaxAmount();
            BigDecimal totalAfterTax = result.getTotalAmountAfterTax();
            Map<String, Object> meterPointBreakdowns = result.getMeterPointBreakdowns();
            List<Map<String, Object>> stepDetails = result.getStepDetails();
            Map<String, BigDecimal> nodeNetConsumptions = result.getNodeNetConsumptions();

            log.info("[AUDIT-TRACER] [Account: {}] Step 5.1: Billing engine finished. Total Net Consumption: {}, Total Amount before tax: {}, VAT Tax Amount: {}, Total Amount after tax: {}.", 
                    maKhang, nodeNetConsumptions, totalBeforeTax, taxAmount, totalAfterTax);

            // 5. Construct the BillInvoice entity instead of self-explanatory manifest
            String invoiceId = "INV-" + maKhang + "-" + month + "-v" + version;
            String idempotencyKey = maKhang + "_" + month + "_p" + task.getKyChot() + "_v" + version;
            String maDviqly = config.getMaDviqly();
            if (maDviqly == null || maDviqly.trim().isEmpty()) {
                throw new IllegalStateException("Cấu hình đóng băng (snapshot) thiếu thông tin mã đơn vị quản lý (maDviqly) bắt buộc.");
            }

            // Build chi_tiet_diem_do JSONB array
            List<Map<String, Object>> ctietList = new ArrayList<>();
            BigDecimal totalDienTthu = BigDecimal.ZERO;
            for (MeterUsage u : deduplicatedUsages) {
                String mId = u.getMaDdo();
                String bcs = u.getTgianBdien() != null ? u.getTgianBdien() : "KT";

                MeterPointNode node = findMeterPointNode(config.getMeterTopology().getRootPoints(), mId);
                PriceApplicationRule matchedRule = null;
                if (node != null && node.getPriceRules() != null) {
                    matchedRule = node.getPriceRules().stream()
                            .filter(r -> bcs.equals(r.getTgianBdien()))
                            .findFirst()
                            .orElse(node.getPriceRules().isEmpty() ? null : node.getPriceRules().get(0));
                }

                String soCto = null;
                int soPha = 1;
                Short loaiDdoVal = 1;
                if (node != null) {
                    loaiDdoVal = node.getLoaiDdo();
                    if (node.getActiveMeters() != null && !node.getActiveMeters().isEmpty()) {
                        MeterDetails meter = node.getActiveMeters().get(0);
                        soCto = meter.getSoSeri();
                        soPha = meter.getSoPha();
                    }
                    if (soCto == null) {
                        soCto = node.getMeterSerial();
                    }
                }
                if (soCto == null) {
                    soCto = u.getMaCto();
                }

                // Sum step amounts for this point and bcs
                BigDecimal bcsAmount = BigDecimal.ZERO;
                BigDecimal bcsKwh = BigDecimal.ZERO;
                if (result.getStepDetails() != null) {
                    for (Map<String, Object> sd : result.getStepDetails()) {
                        String sdMeterId = (String) sd.get("meter_point_id");
                        String sdTimePeriod = (String) sd.get("time_period");
                        if (mId.equals(sdMeterId) && bcs.equals(sdTimePeriod)) {
                            BigDecimal kwh = (BigDecimal) sd.get("kwh");
                            BigDecimal amt = (BigDecimal) sd.get("amount");
                            if (kwh != null) bcsKwh = bcsKwh.add(kwh);
                            if (amt != null) bcsAmount = bcsAmount.add(amt);
                        }
                    }
                }

                if (bcsKwh.compareTo(BigDecimal.ZERO) == 0) {
                    bcsKwh = u.getConsumption() != null ? u.getConsumption() : BigDecimal.ZERO;
                }

                BigDecimal donGia = BigDecimal.ZERO;
                if (bcsKwh.compareTo(BigDecimal.ZERO) > 0) {
                    donGia = bcsAmount.divide(bcsKwh, 4, RoundingMode.HALF_UP);
                }

                Map<String, Object> ctiet = new LinkedHashMap<>();
                ctiet.put("ma_ddo", mId);
                ctiet.put("bcs", bcs);
                ctiet.put("tgian_bdien", bcs);
                ctiet.put("ma_nhomnn", matchedRule != null ? matchedRule.getMaNhomnn() : null);
                ctiet.put("ma_nn", null);
                ctiet.put("ma_ngia", matchedRule != null ? matchedRule.getMaNgia() : null);
                ctiet.put("ma_capda", matchedRule != null ? matchedRule.getMaCapda() : null);
                ctiet.put("loai_ddo", loaiDdoVal != null ? loaiDdoVal.intValue() : 1);
                ctiet.put("so_pha", soPha);
                ctiet.put("so_cto", soCto);
                ctiet.put("id_chi_so", u.getIdChiSo());
                ctiet.put("ngay_apdung", config.getNgayHieuLuc() != null ? config.getNgayHieuLuc().toString() : null);
                ctiet.put("dien_tthu", bcsKwh);
                ctiet.put("don_gia", donGia);
                ctiet.put("so_tien", bcsAmount);
                ctiet.put("ty_le", null);
                ctiet.put("tien_gtru", null);
                ctiet.put("tien_goc", null);
                ctiet.put("dinh_muc", matchedRule != null && matchedRule.getDinhMuc() != null ? matchedRule.getDinhMuc().toString() : null);
                ctiet.put("loai_dmuc", matchedRule != null ? matchedRule.getLoaiDmuc() : null);
                ctiet.put("noi_dung", null);
                ctiet.put("cmis_id_hdonctiet", null);

                ctietList.add(ctiet);
                totalDienTthu = totalDienTthu.add(bcsKwh);
            }
            String chiTietDiemDoJson = objectMapper.writeValueAsString(ctietList);

            // Construct BillInvoice object
            BillInvoice invoice = new BillInvoice();
            invoice.setIdHoaDon(invoiceId);
            invoice.setMaKhang(maKhang);
            invoice.setDtuongQly(dtuongQly);
            invoice.setThangChuKy(month);
            invoice.setKyChot(task.getKyChot());
            invoice.setMaDviqly(maDviqly);
            invoice.setSoTien(totalBeforeTax);
            invoice.setTienGtgt(taxAmount);
            invoice.setTyleThue(vatRate.multiply(BigDecimal.valueOf(100)));
            invoice.setTongTien(totalAfterTax);
            invoice.setDienTthu(totalDienTthu);

            // Set date ranges
            LocalDate ngayDkyLocalDate = config.getTuNgay() != null ? config.getTuNgay() : minFrom.toLocalDate();
            LocalDate ngayCkyLocalDate = config.getDenNgay() != null ? config.getDenNgay() : maxTo.toLocalDate();
            invoice.setNgayDky(ngayDkyLocalDate);
            invoice.setNgayCky(ngayCkyLocalDate);
            invoice.setSoHo(config.getSoHo() > 0 ? BigDecimal.valueOf(config.getSoHo()) : BigDecimal.ONE);
            invoice.setLoaiKhang(config.getLoaiKhang() != null ? config.getLoaiKhang().intValue() : 1);
            invoice.setLoaiHdon("TD");
            invoice.setChiTietDiemDo(chiTietDiemDoJson);
            invoice.setKhoaLapTrung(idempotencyKey);
            invoice.setTrangThaiTinhToan("FINAL");
            invoice.setRefSnapshot(maKhang + "_" + month + "_v" + version);

            invoice.setCreatedAt(LocalDateTime.now());
            invoice.setUpdatedAt(LocalDateTime.now());

            saveInvoiceAtomically(invoice, version);

            // Log raw calculate results into nhat_ky_tinh_toan using BillingLogService (asynchronously)
            long duration = System.currentTimeMillis() - tStart;
            Map<String, Object> inputLogMap = new HashMap<>();
            inputLogMap.put("config", config);
            inputLogMap.put("consumptions", consumptions);
            String inputJson = objectMapper.writeValueAsString(inputLogMap);

            Map<String, Object> ratingDump = new HashMap<>();
            ratingDump.put("totalAmountBeforeTax", totalBeforeTax);
            ratingDump.put("taxAmount", taxAmount);
            ratingDump.put("totalAmountAfterTax", totalAfterTax);
            ratingDump.put("stepDetails", stepDetails);
            ratingDump.put("meterPointBreakdowns", meterPointBreakdowns);
            ratingDump.put("nodeNetConsumptions", nodeNetConsumptions);
            String ratingDumpJson = objectMapper.writeValueAsString(ratingDump);

            billingLogService.enqueueLog(
                    invoiceId, month, maKhang, "SUCCESS",
                    inputJson, ratingDumpJson, null, duration, workerNodeId
            );

            // Update calculations tracking statuses
            if (meterRegistry != null) {
                meterRegistry.counter("billing.calculations", "status", "success").increment();
                meterRegistry.timer("billing.execution.time").record(duration, TimeUnit.MILLISECONDS);
            }
            String targetStatus = "SUCCESS";
            if ("BATCH".equals(task.getTriggeredBy()) || "CMIS".equals(task.getTriggeredBy())) {
                targetStatus = "SUCCESS_CMIS";
            }
            boolean statusChanged = updateAccountStatus(dtuongQly, maKhang, month, task.getKyChot(), targetStatus, invoiceId, null, duration, workerNodeId);
            if (statusChanged) {
                updateBookBillingRunProgress(dtuongQly, month, task.getKyChot(), 1, 1, 0);
            }
        } catch (Exception e) {
            long duration = System.currentTimeMillis() - tStart;
            if (meterRegistry != null) {
                meterRegistry.counter("billing.calculations", "status", "error").increment();
                meterRegistry.timer("billing.execution.time").record(duration, TimeUnit.MILLISECONDS);
            }
            boolean statusChanged = updateAccountStatus(dtuongQly, maKhang, month, task.getKyChot(), "FAILED", null, e.getMessage(), duration, workerNodeId);
            if (statusChanged) {
                updateBookBillingRunProgress(dtuongQly, month, task.getKyChot(), 1, 0, 1);
            }
            try {
                selfHealingService.handleFailure(task, e.getMessage());
            } catch (Exception healEx) {
                log.error("[BILLING] SelfHealing handleFailure failed for {}: {}", maKhang, healEx.getMessage());
            }
            billingLogService.enqueueLog(
                    "INV-" + maKhang + "-" + month + "-v" + version, month, maKhang, "FAILED",
                    null, null, e.getMessage(), duration, workerNodeId
            );
            log.error("[BILLING] Calculation failed for account {}: {}", maKhang, e.getMessage(), e);
        }
    }

    /**
     * FIX-01: KHÔNG có @Transactional ở đây.
     * Tính toán là pure CPU — không giữ DB connection.
     * Chỉ batch write ở cuối mới cần transaction (trong commitBillingBatch).
     *
     * FIX-BOOK: Nhóm tasks theo dtuongQly trước khi xử lý để đảm bảo:
     *  - warmupBookCache() đúng cho từng sổ cước.
     *  - Redis hash key và progress counter được ghi đúng sổ.
     *  - checkAndTriggerAutoBatch() không nhầm sổ.
     * Một Kafka batch có thể chứa KH của nhiều sổ cước khác nhau nếu
     * producer không partition strict theo dtuongQly.
     */
    public void processBillingBatch(List<BillingTaskDto> tasks) throws Exception {
        if (tasks == null || tasks.isEmpty()) return;

        // Validate: tất cả tasks phải có dtuongQly
        for (BillingTaskDto t : tasks) {
            if (t.getDtuongQly() == null || t.getDtuongQly().trim().isEmpty()) {
                throw new IllegalArgumentException(
                        "Task trong lô thiếu thông tin đối tượng quản lý (dtuongQly) bắt buộc cho khách hàng: " + t.getMaKhang());
            }
        }

        // FIX-03: Cache VAT rate 1 lần cho toàn bộ lô (thay vì 1000 SELECT)
        BigDecimal vatRate = fetchVatRateWithCache();

        // FIX-BOOK: Nhóm tasks theo (dtuongQly + thangChuKy + kyChot)
        // để đảm bảo warmup, claim, commit đều dùng đúng book key.
        Map<String, List<BillingTaskDto>> tasksByBook = new LinkedHashMap<>();
        for (BillingTaskDto task : tasks) {
            String bookKey = task.getDtuongQly() + ":" + task.getThangChuKy() + ":" + task.getKyChot();
            tasksByBook.computeIfAbsent(bookKey, k -> new ArrayList<>()).add(task);
        }
        log.info("[BATCH-BOOK] Batch of {} tasks grouped into {} book(s): {}",
                tasks.size(), tasksByBook.size(), tasksByBook.keySet());

        // Xử lý từng nhóm sổ cước độc lập
        for (Map.Entry<String, List<BillingTaskDto>> entry : tasksByBook.entrySet()) {
            List<BillingTaskDto> bookTasks = entry.getValue();
            String dtuongQly = bookTasks.get(0).getDtuongQly();
            String month = bookTasks.get(0).getThangChuKy();
            int period = bookTasks.get(0).getKyChot();
            processBillingBatchForBook(bookTasks, dtuongQly, month, period, vatRate);
        }
    }

    /**
     * Xử lý một nhóm tasks thuộc cùng 1 sổ cước (dtuongQly + thangChuKy + kyChot).
     * Tách riêng để đảm bảo mọi I/O (warmup, claim, commit, progress) đều dùng đúng book key.
     */
    private void processBillingBatchForBook(List<BillingTaskDto> tasks,
                                            String dtuongQly, String month, int period,
                                            BigDecimal vatRate) throws Exception {

        warmupBookCache(dtuongQly, month, period);

        // FIX-02: Batch claim toàn bộ account trong 1 SQL duy nhất (thay vì N×REQUIRES_NEW)
        List<String> allMaKhangs = tasks.stream()
                .map(BillingTaskDto::getMaKhang)
                .collect(Collectors.toList());
        List<String> claimedAccounts = billingStateRepository.batchClaimProcessingWorkers(
                allMaKhangs, month, period, workerNodeId, claimTimeoutMinutes);
        Set<String> claimedSet = new HashSet<>(claimedAccounts);
        log.info("[BATCH-CLAIM] Book: {}, Claimed {}/{} accounts.", dtuongQly, claimedAccounts.size(), allMaKhangs.size());

        List<BillInvoice> invoiceBatch = new ArrayList<>();
        List<Object[]> outboxBatch = new ArrayList<>();
        List<Object[]> statusBatch = new ArrayList<>();

        for (BillingTaskDto task : tasks) {
            String maKhang = task.getMaKhang();
            int version = task.getPhienBanTinh();
            // dtuongQly, month, period đã được xác định từ tham số của method — không redeclare

            // FIX-02: Dùng kết quả batch claim thay vì gọi từng lần
            if (!claimedSet.contains(maKhang)) {
                log.info("[SKIP-CALC-BATCH] Account {} not claimed (already processing/finalized) for month {} period {}.",
                        maKhang, month, period);
                continue;
            }

            long tStart = System.currentTimeMillis();
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
                        u.setMaKhang(maKhang);
                        u.setThangChuKy(month);
                        u.setTrangThaiXuLy("VALIDATED");
                        u.setCoQuayVong(r.getCoQuayVong() != null ? r.getCoQuayVong() : false);
                        u.setMaxRegisterSnapshot(r.getMaxRegisterSnapshot());
                        
                        if (r.getLanDocPhu() == null) {
                            throw new IllegalArgumentException("Dữ liệu chỉ số thiếu thông tin lần đọc phụ (lanDocPhu) bắt buộc cho điểm đo: " + r.getMaDdo());
                        }
                        u.setLanDocPhu(r.getLanDocPhu());
                        u.setLoaiGhiIndex(r.getLoaiGhiIndex() != null ? r.getLoaiGhiIndex() : "ORIGINAL");
                        u.setTgianBdien(r.getTgianBdien() != null ? r.getTgianBdien() : "KT");
                        usages.add(u);
                    }
                } else {
                    usages = meterUsageRepository.findByMaKhangAndThangChuKyAndKyChotAndTrangThaiXuLy(
                            maKhang, month, task.getKyChot(), "VALIDATED");
                }
                if (usages.isEmpty()) {
                    throw new NoSuchElementException("No validated meter usage found/provided for account: " + maKhang);
                }

                // Determine actual billing period length (daysUsed)
                LocalDateTime minFrom = null;
                LocalDateTime maxTo = null;
                for (MeterUsage u : usages) {
                    if (u.getTuNgay() != null && (minFrom == null || u.getTuNgay().isBefore(minFrom))) minFrom = u.getTuNgay();
                    if (u.getDenNgay() != null && (maxTo == null || u.getDenNgay().isAfter(maxTo))) maxTo = u.getDenNgay();
                }
                if (minFrom == null) minFrom = LocalDateTime.now().minusDays(30);
                if (maxTo == null) maxTo = LocalDateTime.now();
                long daysUsed = ChronoUnit.DAYS.between(minFrom.toLocalDate(), maxTo.toLocalDate()) + 1;

                int daysInMonth = 30;
                if (month != null && month.contains("_")) {
                    try {
                        String[] parts = month.split("_");
                        YearMonth yearMonth = YearMonth.of(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]));
                        daysInMonth = yearMonth.lengthOfMonth();
                    } catch (Exception ignored) { }
                }

                // 2. Fetch Frozen Snapshot from Redis Cache
                String cacheKey = "snapshot:" + maKhang + ":" + month + ":" + task.getKyChot();
                BillingConfigSnapshot config = null;
                String changeFlags = task.getChangeFlags() != null ? task.getChangeFlags() : "NONE";
                boolean needsInvalidate = "PRICE_CHANGE".equals(changeFlags)
                        || "METER_CHANGE".equals(changeFlags)
                        || "MULTI_CHANGE".equals(changeFlags);

                if (!needsInvalidate && task.getSnapshotVersion() != null) {
                    try {
                        Object cachedObj = redisTemplate.opsForValue().get(cacheKey);
                        if (cachedObj != null) {
                            BillingConfigSnapshot cached = (cachedObj instanceof BillingConfigSnapshot)
                                    ? (BillingConfigSnapshot) cachedObj
                                    : objectMapper.convertValue(cachedObj, BillingConfigSnapshot.class);
                            if (cached.getPhienBanTinh() != null && cached.getPhienBanTinh() < task.getSnapshotVersion()) {
                                needsInvalidate = true;
                                log.info("[SNAP-STALE-BATCH] Cache version {} < task required version {}. Force invalidate for account: {}",
                                        cached.getPhienBanTinh(), task.getSnapshotVersion(), maKhang);
                            } else {
                                config = cached;
                            }
                        }
                    } catch (Exception e) {
                        log.warn("Redis read failure for account {}: {}", maKhang, e.getMessage());
                    }
                } else if (!needsInvalidate) {
                    try {
                        Object cachedObj = redisTemplate.opsForValue().get(cacheKey);
                        if (cachedObj != null) {
                            config = cachedObj instanceof BillingConfigSnapshot
                                    ? (BillingConfigSnapshot) cachedObj
                                    : objectMapper.convertValue(cachedObj, BillingConfigSnapshot.class);
                        }
                    } catch (Exception e) {
                        log.warn("[SNAP-CACHE] Redis offline for account {}: {}", maKhang, e.getMessage());
                    }
                }

                if (needsInvalidate) {
                    try {
                        redisTemplate.delete(cacheKey);
                        log.info("[SNAP-INVALIDATE-BATCH] Invalidated cache for account: {}", maKhang);
                    } catch (Exception e) {
                        log.warn("Failed to invalidate cache: {}", e.getMessage());
                    }
                }

                if (config == null) {
                    Optional<BillingAccountSnapshot> snapshotOpt = snapshotRepository
                            .findByMaKhangAndThangChuKyAndKyChotAndPhienBanTinh(maKhang, month, task.getKyChot(), version);
                    if (snapshotOpt.isEmpty()) {
                        snapshotOpt = snapshotRepository
                                .findByMaKhangAndThangChuKyAndKyChotAndPhienBanTinh(maKhang, month, task.getKyChot(), 1);
                    }
                    if (snapshotOpt.isEmpty()) {
                        // FIX-04: KHÔNG gọi HTTP sync trong batch transaction.
                        // Throw exception → statusBatch ghi FAILED → DLQ/SelfHealing xử lý sau.
                        throw new NoSuchElementException("Snapshot not found for account: " + maKhang
                                + ". Please pre-generate snapshots before running batch.");
                    }
                    config = snapshotOpt.get().getDuLieuCauHinh();
                    try {
                        redisTemplate.opsForValue().set(cacheKey, config, 24, TimeUnit.HOURS);
                    } catch (Exception ignored) { }
                }

                // FIX-05: Đồng bộ FastPath routing với processBilling() single mode
                changeFlags = task.getChangeFlags() != null ? task.getChangeFlags() : "NONE";
                boolean isFastPath = "NONE".equals(changeFlags)
                        && !task.isHasRelation()
                        && config.isFastPathEnabled()
                        && ("SINH_HOAT".equals(config.getCustomerType())
                                || "NGOAI_SINH_HOAT".equals(config.getCustomerType()));

                // FIX-06: Skip validateSnapshot() cho FastPath — tránh overhead
                if (!isFastPath) {
                    validateSnapshot(config, maKhang);
                } else {
                    log.debug("[ROUTING-BATCH] Skip snapshot validation for FastPath account: {}", maKhang);
                }

                // 3. Collect node consumptions
                Map<String, MeterUsage> latestUsagePerBcs = new LinkedHashMap<>();
                for (MeterUsage u : usages) {
                    String tp = u.getTgianBdien() != null ? u.getTgianBdien() : "KT";
                    String bcsKey = u.getMaDdo() + "|" + tp;
                    latestUsagePerBcs.merge(bcsKey, u, (existing, incoming) -> {
                        int existingSeq = existing.getLanDocPhu() != null ? existing.getLanDocPhu() : 1;
                        int incomingSeq = incoming.getLanDocPhu() != null ? incoming.getLanDocPhu() : 1;
                        return incomingSeq > existingSeq ? incoming : existing;
                    });
                }
                List<MeterUsage> deduplicatedUsages = new ArrayList<>(latestUsagePerBcs.values());

                Map<String, BigDecimal> consumptions = new HashMap<>();
                for (MeterUsage u : deduplicatedUsages) {
                    BigDecimal cons = u.getConsumption() != null ? u.getConsumption()
                            : u.getChiSoCuoi().subtract(u.getChiSoDau());
                    String tp = u.getTgianBdien() != null ? u.getTgianBdien() : "KT";
                    String detailKey = u.getMaDdo() + "_" + tp;
                    consumptions.merge(detailKey, cons, BigDecimal::add);
                    if (!"VC".equals(tp)) {
                        consumptions.merge(u.getMaDdo(), cons, BigDecimal::add);
                    }
                }

                // FIX-03: Dùng vatRate đã cache — KHÔNG query DB trong vòng lặp
                if (config.getSchemaSteps() != null) {
                    for (BillingSchemaStep step : config.getSchemaSteps()) {
                        if ("TAX".equals(step.getVariantName())) {
                            if (step.getStepConfig() == null) step.setStepConfig(new HashMap<>());
                            step.getStepConfig().put("taxRate", vatRate.doubleValue());
                        }
                    }
                }

                // 4. Invoke Core Stateless Rating Engine — FIX-05: Route FastPath
                CalculationResult result = isFastPath
                        ? billingCalculator.calculateFastPath(config, consumptions, month, daysUsed)
                        : billingCalculator.calculate(config, consumptions, month, daysUsed);

                BigDecimal totalBeforeTax = result.getTotalAmountBeforeTax();
                BigDecimal taxAmount = result.getTaxAmount();
                BigDecimal totalAfterTax = result.getTotalAmountAfterTax();
                List<Map<String, Object>> stepDetails = result.getStepDetails();
                Map<String, BigDecimal> nodeNetConsumptions = result.getNodeNetConsumptions();

                // 5. Construct billing manifest
                Map<String, Object> manifest = new HashMap<>();
                String invoiceId = "INV-" + maKhang + "-" + month + "-v" + version;
                manifest.put("invoice_id", invoiceId);
                manifest.put("calculation_engine_version", "v2.1-stable");
                manifest.put("timestamp", LocalDateTime.now().toString());
                manifest.put("snapshot_applied", maKhang + "_" + month + "_v" + version);

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

                Map<String, Object> taxCalcMap = new HashMap<>();
                taxCalcMap.put("vat_rate", vatRate);
                taxCalcMap.put("tax_amount_raw", taxAmount);
                taxCalcMap.put("rounding_mode", "HALF_UP");
                taxCalcMap.put("tax_amount_final", taxAmount);
                manifest.put("tax_calculation", taxCalcMap);
                manifest.put("total_final_amount", totalAfterTax);

                String idempotencyKey = maKhang + "_" + month + "_p" + task.getKyChot() + "_v" + version;
                String maDviqly = config.getMaDviqly();
                if (maDviqly == null || maDviqly.trim().isEmpty()) {
                    throw new IllegalStateException("Cấu hình đóng băng (snapshot) thiếu thông tin mã đơn vị quản lý (maDviqly) bắt buộc.");
                }

                // Build chi_tiet_diem_do JSONB array
                List<Map<String, Object>> ctietList = new ArrayList<>();
                BigDecimal totalDienTthu = BigDecimal.ZERO;
                for (MeterUsage u : deduplicatedUsages) {
                    String mId = u.getMaDdo();
                    String bcs = u.getTgianBdien() != null ? u.getTgianBdien() : "KT";

                    MeterPointNode node = findMeterPointNode(config.getMeterTopology().getRootPoints(), mId);
                    PriceApplicationRule matchedRule = null;
                    if (node != null && node.getPriceRules() != null) {
                        matchedRule = node.getPriceRules().stream()
                                .filter(r -> bcs.equals(r.getTgianBdien()))
                                .findFirst()
                                .orElse(node.getPriceRules().isEmpty() ? null : node.getPriceRules().get(0));
                    }

                    String soCto = null;
                    int soPha = 1;
                    Short loaiDdoVal = 1;
                    if (node != null) {
                        loaiDdoVal = node.getLoaiDdo();
                        if (node.getActiveMeters() != null && !node.getActiveMeters().isEmpty()) {
                            MeterDetails meter = node.getActiveMeters().get(0);
                            soCto = meter.getSoSeri();
                            soPha = meter.getSoPha();
                        }
                        if (soCto == null) {
                            soCto = node.getMeterSerial();
                        }
                    }
                    if (soCto == null) {
                        soCto = u.getMaCto();
                    }

                    // Sum step amounts for this point and bcs
                    BigDecimal bcsAmount = BigDecimal.ZERO;
                    BigDecimal bcsKwh = BigDecimal.ZERO;
                    if (result.getStepDetails() != null) {
                        for (Map<String, Object> sd : result.getStepDetails()) {
                            String sdMeterId = (String) sd.get("meter_point_id");
                            String sdTimePeriod = (String) sd.get("time_period");
                            if (mId.equals(sdMeterId) && bcs.equals(sdTimePeriod)) {
                                BigDecimal kwh = (BigDecimal) sd.get("kwh");
                                BigDecimal amt = (BigDecimal) sd.get("amount");
                                if (kwh != null) bcsKwh = bcsKwh.add(kwh);
                                if (amt != null) bcsAmount = bcsAmount.add(amt);
                            }
                        }
                    }

                    if (bcsKwh.compareTo(BigDecimal.ZERO) == 0) {
                        bcsKwh = u.getConsumption() != null ? u.getConsumption() : BigDecimal.ZERO;
                    }

                    BigDecimal donGia = BigDecimal.ZERO;
                    if (bcsKwh.compareTo(BigDecimal.ZERO) > 0) {
                        donGia = bcsAmount.divide(bcsKwh, 4, RoundingMode.HALF_UP);
                    }

                    Map<String, Object> ctiet = new LinkedHashMap<>();
                    ctiet.put("ma_ddo", mId);
                    ctiet.put("bcs", bcs);
                    ctiet.put("tgian_bdien", bcs);
                    ctiet.put("ma_nhomnn", matchedRule != null ? matchedRule.getMaNhomnn() : null);
                    ctiet.put("ma_nn", null);
                    ctiet.put("ma_ngia", matchedRule != null ? matchedRule.getMaNgia() : null);
                    ctiet.put("ma_capda", matchedRule != null ? matchedRule.getMaCapda() : null);
                    ctiet.put("loai_ddo", loaiDdoVal != null ? loaiDdoVal.intValue() : 1);
                    ctiet.put("so_pha", soPha);
                    ctiet.put("so_cto", soCto);
                    ctiet.put("id_chi_so", u.getIdChiSo());
                    ctiet.put("ngay_apdung", config.getNgayHieuLuc() != null ? config.getNgayHieuLuc().toString() : null);
                    ctiet.put("dien_tthu", bcsKwh);
                    ctiet.put("don_gia", donGia);
                    ctiet.put("so_tien", bcsAmount);
                    ctiet.put("ty_le", null);
                    ctiet.put("tien_gtru", null);
                    ctiet.put("tien_goc", null);
                    ctiet.put("dinh_muc", matchedRule != null && matchedRule.getDinhMuc() != null ? matchedRule.getDinhMuc().toString() : null);
                    ctiet.put("loai_dmuc", matchedRule != null ? matchedRule.getLoaiDmuc() : null);
                    ctiet.put("noi_dung", null);
                    ctiet.put("cmis_id_hdonctiet", null);

                    ctietList.add(ctiet);
                    totalDienTthu = totalDienTthu.add(bcsKwh);
                }
                String chiTietDiemDoJson = objectMapper.writeValueAsString(ctietList);

                // Construct BillInvoice object
                BillInvoice invoice = new BillInvoice();
                invoice.setIdHoaDon(invoiceId);
                invoice.setMaKhang(maKhang);
                invoice.setDtuongQly(dtuongQly);
                invoice.setThangChuKy(month);
                invoice.setKyChot(task.getKyChot());
                invoice.setMaDviqly(maDviqly);
                invoice.setSoTien(totalBeforeTax);
                invoice.setTienGtgt(taxAmount);
                invoice.setTyleThue(vatRate.multiply(BigDecimal.valueOf(100)));
                invoice.setTongTien(totalAfterTax);
                invoice.setDienTthu(totalDienTthu);

                // Set date ranges
                LocalDate ngayDkyLocalDate = config.getTuNgay() != null ? config.getTuNgay() : minFrom.toLocalDate();
                LocalDate ngayCkyLocalDate = config.getDenNgay() != null ? config.getDenNgay() : maxTo.toLocalDate();
                invoice.setNgayDky(ngayDkyLocalDate);
                invoice.setNgayCky(ngayCkyLocalDate);
                invoice.setSoHo(config.getSoHo() > 0 ? BigDecimal.valueOf(config.getSoHo()) : BigDecimal.ONE);
                invoice.setLoaiKhang(config.getLoaiKhang() != null ? config.getLoaiKhang().intValue() : 1);
                invoice.setLoaiHdon("TD");
                invoice.setChiTietDiemDo(chiTietDiemDoJson);
                invoice.setKhoaLapTrung(idempotencyKey);
                invoice.setTrangThaiTinhToan("FINAL");
                invoice.setRefSnapshot(maKhang + "_" + month + "_v" + version);

                invoice.setCreatedAt(LocalDateTime.now());
                invoice.setUpdatedAt(LocalDateTime.now());

                invoiceBatch.add(invoice);

                Map<String, Object> outboxPayload = new HashMap<>();
                outboxPayload.put("invoiceId", invoiceId);
                outboxPayload.put("maKhang", maKhang);
                outboxPayload.put("billingCycleMonth", month);
                outboxPayload.put("amountBeforeTax", totalBeforeTax);
                outboxPayload.put("taxAmount", taxAmount);
                outboxPayload.put("amountAfterTax", totalAfterTax);
                outboxPayload.put("timestamp", LocalDateTime.now().toString());
                outboxBatch.add(new Object[] {
                    UUID.randomUUID(), "INVOICE", invoiceId, "INVOICE_CREATED",
                    objectMapper.writeValueAsString(outboxPayload),
                    Timestamp.valueOf(LocalDateTime.now())
                });

                String targetStatus = "SUCCESS";
                if ("BATCH".equals(task.getTriggeredBy()) || "CMIS".equals(task.getTriggeredBy())) {
                    targetStatus = "SUCCESS_CMIS";
                }
                if (totalBeforeTax != null && totalBeforeTax.compareTo(BigDecimal.valueOf(anomalyThresholdVnd)) > 0) {
                    targetStatus = "ANOMALY";
                }
                statusBatch.add(new Object[] {
                    maKhang, month, dtuongQly, task.getKyChot(),
                    targetStatus, invoiceId, null, workerNodeId,
                    Timestamp.valueOf(LocalDateTime.now())
                });

                Map<String, Object> inputLogMap = new HashMap<>();
                inputLogMap.put("config", config);
                inputLogMap.put("consumptions", consumptions);
                String inputJson = objectMapper.writeValueAsString(inputLogMap);

                Map<String, Object> ratingDump = new HashMap<>();
                ratingDump.put("totalAmountBeforeTax", totalBeforeTax);
                ratingDump.put("taxAmount", taxAmount);
                ratingDump.put("totalAmountAfterTax", totalAfterTax);
                ratingDump.put("stepDetails", stepDetails);
                ratingDump.put("meterPointBreakdowns", result.getMeterPointBreakdowns());
                ratingDump.put("nodeNetConsumptions", nodeNetConsumptions);
                String ratingDumpJson = objectMapper.writeValueAsString(ratingDump);

                long duration = System.currentTimeMillis() - tStart;
                billingLogService.enqueueLog(
                        invoiceId, month, maKhang, targetStatus,
                        inputJson, ratingDumpJson, null, duration, workerNodeId
                );

            } catch (Exception e) {
                log.error("Calculation failed for account: {}, error: {}", maKhang, e.getMessage(), e);
                statusBatch.add(new Object[] {
                    maKhang, month, dtuongQly, task.getKyChot(),
                    "FAILED", null, e.getMessage(), workerNodeId,
                    Timestamp.valueOf(LocalDateTime.now())
                });
                try {
                    selfHealingService.handleFailure(task, e.getMessage());
                } catch (Exception healEx) {
                    log.error("[BILLING-BATCH] SelfHealing handleFailure failed for {}: {}", maKhang, healEx.getMessage());
                }
                long duration = System.currentTimeMillis() - tStart;
                billingLogService.enqueueLog(
                        "INV-" + maKhang + "-" + month + "-v" + version, month, maKhang, "FAILED",
                        null, null, e.getMessage(), duration, workerNodeId
                );
            }
        }

        // FIX-01: Batch writes trong transaction ngắn riêng — tách hoàn toàn khỏi tính toán
        if (!invoiceBatch.isEmpty() || !statusBatch.isEmpty()) {
            commitBillingBatch(invoiceBatch, outboxBatch, statusBatch, dtuongQly, month, period);
        }
    }

    /**
     * FIX-01: Transaction ngắn chỉ dành cho I/O ghi DB.
     * Nếu batch write fail → chỉ rollback 3 bulk SQL, không ảnh hưởng tới tính toán.
     * Kafka sẽ replay lô này và idempotency (ON CONFLICT DO UPDATE) đảm bảo không duplicate.
     */
    @Transactional
    public void commitBillingBatch(List<BillInvoice> invoiceBatch, List<Object[]> outboxBatch,
                                    List<Object[]> statusBatch, String dtuongQly, String month, int period) {
        if (!invoiceBatch.isEmpty()) {
            billingStateRepository.batchUpsertInvoices(invoiceBatch);
            billingStateRepository.batchInsertOutbox(outboxBatch);
            log.info("[AUDIT-TRACER] Batch committed. Saved {} invoices & outbox events to Postgres.", invoiceBatch.size());
        }
        if (!statusBatch.isEmpty()) {
            billingStateRepository.batchUpsertStatuses(statusBatch);

            String hashKey = "billing:book_status_hash:" + dtuongQly + ":" + month + ":" + period;
            Map<String, String> localMap = localBookStatusCache.get(dtuongQly + ":" + month + ":" + period);
            Map<String, String> redisUpdates = new HashMap<>();
            int processedDelta = 0, successDelta = 0, failedDelta = 0;

            for (Object[] row : statusBatch) {
                String accId = (String) row[0];
                String stat = (String) row[4];
                if (localMap != null) localMap.put(accId, stat);
                redisUpdates.put(accId, stat);
                processedDelta++;
                if (Arrays.asList("SUCCESS", "SUCCESS_CMIS", "ANOMALY", "LOCKED", "E_INVOICE_ISSUED").contains(stat)) {
                    successDelta++;
                } else if ("FAILED".equals(stat)) {
                    failedDelta++;
                }
            }
            try {
                if (!redisUpdates.isEmpty()) redisTemplate.opsForHash().putAll(hashKey, redisUpdates);
            } catch (Exception ignored) { }

            updateBookBillingRunProgress(dtuongQly, month, period, processedDelta, successDelta, failedDelta);
            log.info("[AUDIT-TRACER] Status written for {} accounts. Success: {}, Failed: {}.",
                    processedDelta, successDelta, failedDelta);
            checkAndTriggerAutoBatch(dtuongQly, month, period);
        }
    }

    private void validateSnapshot(BillingConfigSnapshot config, String maKhang) {
        if (config == null) {
            throw new com.evn.billing.worker.exception.MalformSnapshotException("Snapshot config is null for account: " + maKhang);
        }
        if (config.getMaKhang() == null || config.getMaKhang().isEmpty()) {
            throw new com.evn.billing.worker.exception.MalformSnapshotException("Missing maKhang in snapshot config for account: " + maKhang);
        }
        if (config.getDtuongQly() == null || config.getDtuongQly().isEmpty()) {
            throw new com.evn.billing.worker.exception.MalformSnapshotException("Missing dtuongQly in snapshot config for account: " + maKhang);
        }
        if (config.getNgayHieuLuc() == null) {
            throw new com.evn.billing.worker.exception.MalformSnapshotException("Missing effectiveSyncDate in snapshot config for account: " + maKhang);
        }
        if (config.getMeterTopology() == null || config.getMeterTopology().getRootPoints() == null || config.getMeterTopology().getRootPoints().isEmpty()) {
            throw new com.evn.billing.worker.exception.MalformSnapshotException("Missing or empty meterTopology in snapshot config for account: " + maKhang);
        }
        if (config.getBieuGia() == null || config.getBieuGia().isEmpty()) {
            throw new com.evn.billing.worker.exception.MalformSnapshotException("Missing or empty tariffs in snapshot config for account: " + maKhang);
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
    public void lockBilling(String maKhang, String month, int period, String targetStatus) throws Exception {
        Map<String, Object> row;
        try {
            row = billingStateRepository.findStatusRowForUpdate(maKhang, month, period);
        } catch (Exception e) {
            throw new NoSuchElementException("Không tìm thấy thông tin cước cho khách hàng: " + maKhang + ", kỳ: " + month + ", đợt: " + period);
        }

        String current = (String) row.get("trang_thai");
        String dtuongQly = (String) row.get("dtuong_qly");

        if (targetStatus.equals(current)) {
            log.info("[LOCK-BILL] Account {} already in target status {} for kỳ: {}, đợt: {}", maKhang, targetStatus, month, period);
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
            throw new IllegalStateException("Không thể chuyển trạng thái từ " + current + " sang " + targetStatus + " cho khách hàng " + maKhang);
        }

        billingStateRepository.updateAccountStatus(targetStatus, maKhang, month, period);

        // Update Redis Cache
        String hashKey = "billing:book_status_hash:" + dtuongQly + ":" + month + ":" + period;
        try {
            redisTemplate.opsForHash().put(hashKey, maKhang, targetStatus);
        } catch (Exception e) {
            log.warn("[LOCK-BILL] Failed to update Redis status to {}: {}", targetStatus, e.getMessage());
        }

        // Update local JVM cache
        String localKey = dtuongQly + ":" + month + ":" + period;
        Map<String, String> localMap = localBookStatusCache.get(localKey);
        if (localMap != null) {
            localMap.put(maKhang, targetStatus);
        }
        log.info("[LOCK-BILL] Locked status of Account: {} to {} for kỳ: {}, đợt: {}", maKhang, targetStatus, month, period);
    }

    public void cancelBilling(String maKhang, String month, int period) throws Exception {
        cancelBillingService.cancelBilling(maKhang, month, period, this, "SYSTEM", null, "REST_API");
    }

    public void cancelBilling(String maKhang, String month, int period,
                              String nguoiHuy, String lyDoHuy, String nguonHuy) throws Exception {
        cancelBillingService.cancelBilling(maKhang, month, period, this, nguoiHuy, lyDoHuy, nguonHuy);
    }

    /**
     * Cập nhật Redis hash và local JVM cache về trạng thái CANCELLED.
     * Gọi từ CancelBillingService — không có @Transactional (fail-safe: lỗi cache không rollback DB).
     */
    public void updateCancelStatusCaches(String dtuongQly, String maKhang, String month, int period) {
        String hashKey = "billing:book_status_hash:" + dtuongQly + ":" + month + ":" + period;
        try {
            redisTemplate.opsForHash().put(hashKey, maKhang, "CANCELLED");
        } catch (Exception e) {
            log.warn("[CANCEL-BILL] Failed to update Redis status to CANCELLED: {}", e.getMessage());
        }
        String localKey = dtuongQly + ":" + month + ":" + period;
        Map<String, String> localMap = localBookStatusCache.get(localKey);
        if (localMap != null) {
            localMap.put(maKhang, "CANCELLED");
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
 
    public Page<AccountBillingStatus> getAccountsByStatus(
            String dtuongQly, String month, int period, List<String> statuses, Pageable pageable) {
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

    public void cancelBookBilling(String dtuongQly, String month, int period) throws Exception {
        log.info("[CANCEL-BOOK-BILL] Request to cancel all billing for Book: {}, Month: {}, Period: {}", 
                dtuongQly, month, period);
        List<String> accountIds = billingStateRepository.findCancelableAccountsForBook(dtuongQly, month, period);
        if (accountIds.isEmpty()) {
            log.info("[CANCEL-BOOK-BILL] No cancelable billing records found for Book: {}, Month: {}, Period: {}", 
                    dtuongQly, month, period);
            return;
        }
        int successCount = 0;
        for (String maKhang : accountIds) {
            try {
                cancelBilling(maKhang, month, period, "SYSTEM", null, "KAFKA");
                successCount++;
            } catch (Exception e) {
                log.error("[CANCEL-BOOK-BILL] Failed to cancel billing for Account: {} in Book: {}", maKhang, dtuongQly, e);
            }
        }
        log.info("[CANCEL-BOOK-BILL] Successfully cancelled {} out of {} accounts of Book: {}", 
                successCount, accountIds.size(), dtuongQly);
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
 
                    kafkaTemplate.send("billing-auto-batch-topic", dtuongQly, autoBatchEvent);
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
                cancelBilling(accId, month, period, "SYSTEM", "CMIS Rejected", "CMIS_REJECT");
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

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void saveInvoiceAtomically(BillInvoice invoice, int version) {
        billingStateRepository.upsertInvoice(invoice);
        billingStateRepository.lockSnapshot(invoice.getMaKhang(), invoice.getThangChuKy(), invoice.getKyChot(), version);
        
        // Construct outbox event payload
        Map<String, Object> outboxPayload = new HashMap<>();
        outboxPayload.put("invoiceId", invoice.getIdHoaDon());
        outboxPayload.put("maKhang", invoice.getMaKhang());
        outboxPayload.put("billingCycleMonth", invoice.getThangChuKy());
        outboxPayload.put("amountBeforeTax", invoice.getSoTien());
        outboxPayload.put("taxAmount", invoice.getTienGtgt());
        outboxPayload.put("amountAfterTax", invoice.getTongTien());
        outboxPayload.put("timestamp", LocalDateTime.now().toString());

        try {
            billingStateRepository.insertOutboxEvent(
                    UUID.randomUUID(), "INVOICE", invoice.getIdHoaDon(), "INVOICE_CREATED",
                    objectMapper.writeValueAsString(outboxPayload), Timestamp.valueOf(LocalDateTime.now()));
        } catch (Exception ex) {
            throw new RuntimeException("Failed to serialize outbox event payload", ex);
        }
        
        log.info("[AUDIT-TRACER] [Account: {}] Step 6: Atomic transaction committed. Invoice + Outbox + Snapshot lock.", invoice.getMaKhang());
    }



    private MeterPointNode findMeterPointNode(List<MeterPointNode> nodes, String maDdo) {
        if (nodes == null || nodes.isEmpty()) return null;
        for (MeterPointNode node : nodes) {
            if (maDdo.equals(node.getMaDdo())) {
                return node;
            }
            MeterPointNode found = findMeterPointNode(node.getChildPoints(), maDdo);
            if (found != null) {
                return found;
            }
        }
        return null;
    }
}
