package com.evn.billing.mediation.job;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.evn.billing.common.dto.BillingTaskDto;
import com.evn.billing.common.dto.MeterReadingDto;
import com.evn.billing.mediation.dto.CmisReadingEvent;
import com.evn.billing.mediation.listener.CmisIngestionListener;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

@Component
@Slf4j
public class OracleAmrIngestionJob {

    @Autowired
    private com.evn.billing.mediation.repository.AmrIngestionRepository amrIngestionRepository;

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private CmisIngestionListener cmisIngestionListener;

    @Autowired
    private com.evn.billing.mediation.config.DynamicIngestionThrottleConfig throttleConfig;

    private final ObjectMapper objectMapper = new ObjectMapper();

    // Fixed Thread Pool executor for compatibility with Java 17 (virtual threads are preview-only)
    private final ExecutorService executor = Executors.newFixedThreadPool(10);

    public static class PendingReadingTarget {
        public final String meterId;
        public final String bcs;
        public final String accountId;
        public final String maCto;
        public final java.math.BigDecimal heSoNhan;

        public PendingReadingTarget(String meterId, String bcs, String accountId, String maCto, java.math.BigDecimal heSoNhan) {
            this.meterId = meterId;
            this.bcs = bcs;
            this.accountId = accountId;
            this.maCto = maCto;
            this.heSoNhan = heSoNhan;
        }
    }

    private List<Map<String, Object>> parseMeterList(String infoCto) {
        if (infoCto == null || infoCto.isEmpty() || "[]".equals(infoCto) || "{}".equals(infoCto)) {
            return Collections.emptyList();
        }
        try {
            if (infoCto.trim().startsWith("[")) {
                return objectMapper.readValue(infoCto, List.class);
            } else {
                Map<String, Object> single = objectMapper.readValue(infoCto, Map.class);
                return List.of(single);
            }
        } catch (Exception e) {
            log.warn("Failed to parse thong_tin_cto JSON: {}", e.getMessage());
        }
        return Collections.emptyList();
    }

    private List<String> lookupBcsFromLoaiDdo(int loaiDdo) {
        switch (loaiDdo) {
            case 2: return List.of("BT", "TD");
            case 3: return List.of("BT", "CD", "TD");
            case 4: return List.of("KT", "VC");
            case 5: return List.of("BT", "TD", "VC");
            case 6: return List.of("BT", "CD", "TD", "VC");
            default: return List.of("KT");
        }
    }

    /**
     * Periodic job to pull AMR readings from Oracle DB mock table.
     * Runs every 30 seconds for simulation.
     */
    @Scheduled(fixedDelay = 30000)
    public void runIngestion() {
        log.info("[ORACLE-AMR-JOB] Starting periodic AMR ingestion job...");
        try {
            // Ensure schema evolved
            amrIngestionRepository.migrateSchema();

            // 1. Fetch active schedules from Repository
            List<Map<String, Object>> activeSchedules = amrIngestionRepository.findActivePendingSchedules();

            if (activeSchedules.isEmpty()) {
                log.info("[ORACLE-AMR-JOB] No active book schedules found. Skipping Ingestion.");
                return;
            }

            for (Map<String, Object> schedule : activeSchedules) {
                String dtuongQly = (String) schedule.get("dtuong_qly");
                String month = (String) schedule.get("thang_ck");
                int period = ((Number) schedule.get("ky_chot")).intValue();
                LocalDate denNgay = null;
                Object denNgayObj = schedule.get("den_ngay");
                if (denNgayObj instanceof java.sql.Date) {
                    denNgay = ((java.sql.Date) denNgayObj).toLocalDate();
                } else if (denNgayObj instanceof LocalDate) {
                    denNgay = (LocalDate) denNgayObj;
                }

                int nTru = ((Number) schedule.get("n_tru")).intValue();
                int nCong = ((Number) schedule.get("n_cong")).intValue();

                if (denNgay == null) continue;

                LocalDate today = LocalDate.now();
                LocalDate minDate = denNgay.minusDays(nTru);
                LocalDate maxDate = denNgay.plusDays(nCong);

                // Only pull readings if today is at or after the start of the tolerance window
                if (today.isBefore(minDate)) {
                    log.info("[ORACLE-AMR-JOB] Today ({}) is before tolerance window start ({}). Skipping book: {}.", 
                            today, minDate, dtuongQly);
                    continue;
                }

                log.info("[ORACLE-AMR-JOB] Pulling readings for book: {}, Month: {}, Period: {}, Target date: {}", 
                        dtuongQly, month, period, denNgay);

                // 2. Fetch all active meter points belonging to this book
                List<Map<String, Object>> activeMeters = amrIngestionRepository.findActiveMetersByDtuongQly(dtuongQly);

                if (activeMeters.isEmpty()) {
                    log.info("[ORACLE-AMR-JOB] No active meter points found for book: {}.", dtuongQly);
                    continue;
                }

                // 3. Fetch all readings already ingested for this book, month, and period
                List<Map<String, Object>> existingReadings = amrIngestionRepository.findIngestedReadings(month, period);

                // Map of meter point ID -> set of already ingested (BCS + ":" + ma_cto)
                Map<String, Set<String>> ingestedMap = new HashMap<>();
                for (Map<String, Object> r : existingReadings) {
                    String mId = (String) r.get("ma_ddo");
                    String bcs = (String) r.get("tgian_bdien");
                    String maCto = (String) r.get("ma_cto");
                    if (maCto == null) maCto = "UNKNOWN";
                    ingestedMap.computeIfAbsent(mId, k -> new HashSet<>()).add(bcs + ":" + maCto);
                }

                // Determine which specific (meterPoint, bcs, accountId) are pending
                List<PendingReadingTarget> pendingTargets = new ArrayList<>();
                for (Map<String, Object> m : activeMeters) {
                    String meterId = (String) m.get("ma_ddo");
                    String accountId = (String) m.get("ma_khang");
                    String infoCto = (String) m.get("thong_tin_cto");
                    int loaiDdo = m.containsKey("loai_ddo") && m.get("loai_ddo") != null ? ((Number) m.get("loai_ddo")).intValue() : 1;

                    List<Map<String, Object>> meterList = parseMeterList(infoCto);
                    if (meterList.isEmpty()) {
                        Map<String, Object> dummy = new HashMap<>();
                        dummy.put("so_seri", "UNKNOWN");
                        dummy.put("ma_cto", "UNKNOWN");
                        dummy.put("he_so_nhan", 1.0);
                        dummy.put("trang_thai", "ACTIVE");
                        dummy.put("danh_sach_bcs", lookupBcsFromLoaiDdo(loaiDdo));
                        meterList = List.of(dummy);
                    }

                    for (Map<String, Object> cto : meterList) {
                        String ctoStatus = (String) cto.get("trang_thai");
                        String ngayTreoStr = (String) cto.get("ngay_treo");
                        String ngayThaoStr = (String) cto.get("ngay_thao");
                        
                        LocalDate ngayTreo = ngayTreoStr != null ? LocalDate.parse(ngayTreoStr) : null;
                        LocalDate ngayThao = ngayThaoStr != null ? LocalDate.parse(ngayThaoStr) : null;

                        boolean isActiveInPeriod = false;
                        if ("ACTIVE".equalsIgnoreCase(ctoStatus)) {
                            isActiveInPeriod = true;
                        } else {
                            if (ngayThao != null && !ngayThao.isBefore(minDate)) {
                                isActiveInPeriod = true;
                            }
                        }

                        if (isActiveInPeriod) {
                            List<String> bcsList = (List<String>) cto.get("danh_sach_bcs");
                            if (bcsList == null || bcsList.isEmpty()) {
                                bcsList = lookupBcsFromLoaiDdo(loaiDdo);
                            }
                            String maCto = (String) cto.getOrDefault("ma_cto", cto.get("so_seri"));
                            if (maCto == null) maCto = "UNKNOWN";

                            Number multNum = (Number) cto.getOrDefault("he_so_nhan", 1.0);
                            BigDecimal heSoNhan = BigDecimal.valueOf(multNum.doubleValue());

                            Set<String> ingestedBcs = ingestedMap.getOrDefault(meterId, Collections.emptySet());

                            for (String bcs : bcsList) {
                                String checkKey = bcs + ":" + maCto;
                                if (!ingestedBcs.contains(checkKey)) {
                                    pendingTargets.add(new PendingReadingTarget(meterId, bcs, accountId, maCto, heSoNhan));
                                }
                            }
                        }
                    }
                }

                if (pendingTargets.isEmpty()) {
                    log.info("[ORACLE-AMR-JOB] All meter point registers in book: {} already have chốt kỳ readings. Skip pulling.", dtuongQly);
                    continue;
                }

                log.info("[ORACLE-AMR-JOB] Found {} pending meter registers in book: {} to pull from Oracle.", pendingTargets.size(), dtuongQly);

                // Calculate Dynamic Throttle Profile based on backlog and SLA
                long pendingCount = pendingTargets.size();
                LocalDateTime deadline = denNgay.atTime(17, 0, 0); // 17:00 of denNgay
                long secondsToSla = java.time.Duration.between(LocalDateTime.now(), deadline).toSeconds();
                
                com.evn.billing.mediation.config.DynamicIngestionThrottleConfig.IngestionProfile profile = throttleConfig.getProfile(pendingCount, secondsToSla);
                log.info("[ORACLE-AMR-JOB] Active Ingestion Profile: {} (Pending: {}, Concurrency: {}, BatchSize: {})", 
                        profile.getName(), pendingCount, profile.getMaxConcurrency(), profile.getBatchSize());

                int chunkSize = profile.getBatchSize();
                long sleepMs = 0;
                if (profile.getRateLimitPerSecond() > 0) {
                    sleepMs = (long) ((chunkSize / profile.getRateLimitPerSecond()) * 1000);
                }

                List<List<PendingReadingTarget>> chunks = new ArrayList<>();
                for (int i = 0; i < pendingTargets.size(); i += chunkSize) {
                    chunks.add(pendingTargets.subList(i, Math.min(i + chunkSize, pendingTargets.size())));
                }

                final String finalMonth = month;
                final int finalPeriod = period;
                final LocalDate finalMinDate = minDate;
                final LocalDate finalMaxDate = maxDate;
                final LocalDate finalDenNgay = denNgay;

                ExecutorService dynamicExecutor = Executors.newFixedThreadPool(profile.getMaxConcurrency());
                List<CompletableFuture<Void>> futures = new ArrayList<>();

                for (List<PendingReadingTarget> chunk : chunks) {
                    CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                        ingestChunkFromOracle(chunk, finalMonth, finalPeriod, finalMinDate, finalMaxDate, finalDenNgay);
                    }, dynamicExecutor);
                    futures.add(future);
                    if (sleepMs > 0) {
                        try {
                            Thread.sleep(sleepMs);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                }

                CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
                dynamicExecutor.shutdown();

                // Re-check pending targets to decide WAITING_AMR or COMPLETED
                List<Map<String, Object>> reCheckReadings = amrIngestionRepository.findIngestedReadings(month, period);
                Map<String, Set<String>> reCheckIngestedMap = new HashMap<>();
                for (Map<String, Object> r : reCheckReadings) {
                    String mId = (String) r.get("ma_ddo");
                    String bcs = (String) r.get("tgian_bdien");
                    String maCto = (String) r.get("ma_cto");
                    if (maCto == null) maCto = "UNKNOWN";
                    reCheckIngestedMap.computeIfAbsent(mId, k -> new HashSet<>()).add(bcs + ":" + maCto);
                }

                boolean stillHasPending = false;
                Set<String> failedAccountIds = new HashSet<>();
                for (Map<String, Object> m : activeMeters) {
                    String meterId = (String) m.get("ma_ddo");
                    String accountId = (String) m.get("ma_khang");
                    String infoCto = (String) m.get("thong_tin_cto");
                    int loaiDdo = m.containsKey("loai_ddo") && m.get("loai_ddo") != null ? ((Number) m.get("loai_ddo")).intValue() : 1;

                    List<Map<String, Object>> meterList = parseMeterList(infoCto);
                    if (meterList.isEmpty()) {
                        Map<String, Object> dummy = new HashMap<>();
                        dummy.put("so_seri", "UNKNOWN");
                        dummy.put("ma_cto", "UNKNOWN");
                        dummy.put("he_so_nhan", 1.0);
                        dummy.put("trang_thai", "ACTIVE");
                        dummy.put("danh_sach_bcs", lookupBcsFromLoaiDdo(loaiDdo));
                        meterList = List.of(dummy);
                    }

                    Set<String> ingestedBcs = reCheckIngestedMap.getOrDefault(meterId, Collections.emptySet());

                    for (Map<String, Object> cto : meterList) {
                        String ctoStatus = (String) cto.get("trang_thai");
                        String ngayThaoStr = (String) cto.get("ngay_thao");
                        LocalDate ngayThao = ngayThaoStr != null ? LocalDate.parse(ngayThaoStr) : null;

                        boolean isActiveInPeriod = "ACTIVE".equalsIgnoreCase(ctoStatus) || (ngayThao != null && !ngayThao.isBefore(minDate));
                        if (isActiveInPeriod) {
                            List<String> bcsList = (List<String>) cto.get("danh_sach_bcs");
                            if (bcsList == null || bcsList.isEmpty()) {
                                bcsList = lookupBcsFromLoaiDdo(loaiDdo);
                            }
                            String maCto = (String) cto.getOrDefault("ma_cto", cto.get("so_seri"));
                            if (maCto == null) maCto = "UNKNOWN";

                            for (String bcs : bcsList) {
                                String checkKey = bcs + ":" + maCto;
                                if (!ingestedBcs.contains(checkKey)) {
                                    stillHasPending = true;
                                    failedAccountIds.add(accountId);
                                }
                            }
                        }
                    }
                }

                if (stillHasPending) {
                    if (today.isBefore(maxDate) || today.equals(maxDate)) {
                        // In retry window, keep WAITING_AMR, do not fail yet
                        amrIngestionRepository.updateScheduleStatus(dtuongQly, month, period, "WAITING_AMR");
                        log.info("[ORACLE-AMR-JOB] Book {} still has pending readings. Tolerance window active until {}. Schedule updated to WAITING_AMR.", dtuongQly, maxDate);
                    } else {
                        // SLA Cutoff breached! Close schedule and fail accounts
                        amrIngestionRepository.updateScheduleStatus(dtuongQly, month, period, "COMPLETED");
                        log.warn("[ORACLE-AMR-JOB] SLA Cutoff reached (Max date: {}). Failing {} accounts due to missing AMR readings.", maxDate, failedAccountIds.size());
                        for (String accId : failedAccountIds) {
                            amrIngestionRepository.updateCustomerBillingStatus(accId, month, period, "INCOMPLETE", "Missing AMR readings after deadline cutoff");
                            amrIngestionRepository.logIncompleteStatus(accId, month, dtuongQly, period, "MISSING_AMR_READING");
                        }
                    }
                } else {
                    // All successfully ingested
                    amrIngestionRepository.updateScheduleStatus(dtuongQly, month, period, "COMPLETED");
                    log.info("[ORACLE-AMR-JOB] All readings ingested successfully. Book {} status updated to COMPLETED.", dtuongQly);
                }
            }

        } catch (Exception e) {
            log.error("[ORACLE-AMR-JOB] Critical error running AMR ingestion job: {}", e.getMessage(), e);
        }
    }

    public void runIngestionForBook(String dtuongQly, String month, int period) {
        log.info("[ORACLE-AMR-JOB] Manually triggering Ingestion for Book: {}, Month: {}, Period: {}", dtuongQly, month, period);
        try {
            amrIngestionRepository.migrateSchema();
            
            List<Map<String, Object>> schedules = amrIngestionRepository.findScheduleTolerance(dtuongQly, month, period);
            if (schedules.isEmpty()) {
                log.warn("[ORACLE-AMR-JOB] No active schedule found for Book: {}, Month: {}, Period: {}", dtuongQly, month, period);
                return;
            }

            Map<String, Object> schedule = schedules.get(0);
            LocalDate denNgay = null;
            Object denNgayObj = schedule.get("den_ngay");
            if (denNgayObj instanceof java.sql.Date) {
                denNgay = ((java.sql.Date) denNgayObj).toLocalDate();
            } else if (denNgayObj instanceof LocalDate) {
                denNgay = (LocalDate) denNgayObj;
            }

            int nTru = ((Number) schedule.get("n_tru")).intValue();
            int nCong = ((Number) schedule.get("n_cong")).intValue();

            if (denNgay == null) return;

            LocalDate minDate = denNgay.minusDays(nTru);
            LocalDate maxDate = denNgay.plusDays(nCong);

            List<Map<String, Object>> activeMeters = amrIngestionRepository.findActiveMetersByDtuongQly(dtuongQly);
            if (activeMeters.isEmpty()) {
                log.info("[ORACLE-AMR-JOB] No active meters found for book: {}", dtuongQly);
                return;
            }

            List<Map<String, Object>> existingReadings = amrIngestionRepository.findIngestedReadings(month, period);
            Map<String, Set<String>> ingestedMap = new HashMap<>();
            for (Map<String, Object> r : existingReadings) {
                String mId = (String) r.get("ma_ddo");
                String bcs = (String) r.get("tgian_bdien");
                String maCto = (String) r.get("ma_cto");
                if (maCto == null) maCto = "UNKNOWN";
                ingestedMap.computeIfAbsent(mId, k -> new HashSet<>()).add(bcs + ":" + maCto);
            }

            List<PendingReadingTarget> pendingTargets = new ArrayList<>();
            for (Map<String, Object> m : activeMeters) {
                String meterId = (String) m.get("ma_ddo");
                String accountId = (String) m.get("ma_khang");
                String infoCto = (String) m.get("thong_tin_cto");
                int loaiDdo = m.containsKey("loai_ddo") && m.get("loai_ddo") != null ? ((Number) m.get("loai_ddo")).intValue() : 1;

                List<Map<String, Object>> meterList = parseMeterList(infoCto);
                if (meterList.isEmpty()) {
                    Map<String, Object> dummy = new HashMap<>();
                    dummy.put("so_seri", "UNKNOWN");
                    dummy.put("ma_cto", "UNKNOWN");
                    dummy.put("he_so_nhan", 1.0);
                    dummy.put("trang_thai", "ACTIVE");
                    dummy.put("danh_sach_bcs", lookupBcsFromLoaiDdo(loaiDdo));
                    meterList = List.of(dummy);
                }

                for (Map<String, Object> cto : meterList) {
                    String ctoStatus = (String) cto.get("trang_thai");
                    String ngayTreoStr = (String) cto.get("ngay_treo");
                    String ngayThaoStr = (String) cto.get("ngay_thao");
                    
                    LocalDate ngayTreo = ngayTreoStr != null ? LocalDate.parse(ngayTreoStr) : null;
                    LocalDate ngayThao = ngayThaoStr != null ? LocalDate.parse(ngayThaoStr) : null;

                    boolean isActiveInPeriod = false;
                    if ("ACTIVE".equalsIgnoreCase(ctoStatus)) {
                        isActiveInPeriod = true;
                    } else {
                        if (ngayThao != null && !ngayThao.isBefore(minDate)) {
                            isActiveInPeriod = true;
                        }
                    }

                    if (isActiveInPeriod) {
                        List<String> bcsList = (List<String>) cto.get("danh_sach_bcs");
                        if (bcsList == null || bcsList.isEmpty()) {
                            bcsList = lookupBcsFromLoaiDdo(loaiDdo);
                        }
                        String maCto = (String) cto.getOrDefault("ma_cto", cto.get("so_seri"));
                        if (maCto == null) maCto = "UNKNOWN";

                        Number multNum = (Number) cto.getOrDefault("he_so_nhan", 1.0);
                        BigDecimal heSoNhan = BigDecimal.valueOf(multNum.doubleValue());

                        Set<String> ingestedBcs = ingestedMap.getOrDefault(meterId, Collections.emptySet());

                        for (String bcs : bcsList) {
                            String checkKey = bcs + ":" + maCto;
                            if (!ingestedBcs.contains(checkKey)) {
                                pendingTargets.add(new PendingReadingTarget(meterId, bcs, accountId, maCto, heSoNhan));
                            }
                        }
                    }
                }
            }

            if (pendingTargets.isEmpty()) {
                log.info("[ORACLE-AMR-JOB] All meters in book: {} already ingested. Skipping.", dtuongQly);
                return;
            }

            int chunkSize = 1000;
            List<List<PendingReadingTarget>> chunks = new ArrayList<>();
            for (int i = 0; i < pendingTargets.size(); i += chunkSize) {
                chunks.add(pendingTargets.subList(i, Math.min(i + chunkSize, pendingTargets.size())));
            }

            final String finalMonth = month;
            final int finalPeriod = period;
            final LocalDate finalMinDate = minDate;
            final LocalDate finalMaxDate = maxDate;
            final LocalDate finalDenNgay = denNgay;

            List<CompletableFuture<Void>> futures = chunks.stream()
                    .map(chunk -> CompletableFuture.runAsync(() -> {
                        ingestChunkFromOracle(chunk, finalMonth, finalPeriod, finalMinDate, finalMaxDate, finalDenNgay);
                    }, executor))
                    .collect(Collectors.toList());

            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
            amrIngestionRepository.updateScheduleStatus(dtuongQly, month, period, "COMPLETED");
            log.info("[ORACLE-AMR-JOB] Manually completed AMR ingestion for book: {}", dtuongQly);
        } catch (Exception e) {
            log.error("[ORACLE-AMR-JOB] Error in manual book ingestion: {}", e.getMessage(), e);
        }
    }



    /**
     * Query Oracle mock table for a specific chunk of meter registers, validate, and batch insert into Postgres.
     */
    private void ingestChunkFromOracle(List<PendingReadingTarget> chunk, String month, int period, 
                                       LocalDate minDate, LocalDate maxDate, LocalDate targetDate) {
        try {
            List<String> meterIds = chunk.stream()
                    .map(t -> t.meterId)
                    .distinct()
                    .collect(Collectors.toList());

            // Retrieve readings from Oracle using Repository
            List<Map<String, Object>> oracleReadings = amrIngestionRepository.findOracleReadings(
                    meterIds, minDate.atStartOfDay(), maxDate.atTime(23, 59, 59)
            );

            if (oracleReadings.isEmpty()) {
                log.debug("[ORACLE-AMR-JOB] No readings found in Oracle for current chunk of {} meters.", meterIds.size());
                return;
            }

            log.info("[ORACLE-AMR-JOB] Retrieved {} readings from Oracle for current chunk.", oracleReadings.size());

            Map<String, Map<String, Object>> readingMap = new HashMap<>();
            for (Map<String, Object> r : oracleReadings) {
                String mId = (String) r.get("ma_ddo");
                String bcs = (String) r.get("bcs");
                readingMap.put(mId + ":" + bcs, r);
            }

            List<Object[]> batchInsertParams = new ArrayList<>();
            Set<String> processedAccounts = new HashSet<>();

            for (PendingReadingTarget target : chunk) {
                String key = target.meterId + ":" + target.bcs;
                Map<String, Object> reading = readingMap.get(key);
                if (reading == null) {
                    continue;
                }

                BigDecimal start = (BigDecimal) reading.get("chi_so_dau");
                BigDecimal end = (BigDecimal) reading.get("chi_so_cuoi");
                Timestamp ngayDoc = (Timestamp) reading.get("ngay_doc");
                boolean coQuayVong = (Boolean) reading.get("co_quay_vong");
                BigDecimal sanLuongTho = (BigDecimal) reading.get("san_luong");

                // Calculate consumption using the specific meter's multiplier (heSoNhan)
                BigDecimal diff = end.subtract(start);
                if (diff.compareTo(BigDecimal.ZERO) < 0 && coQuayVong) {
                    // Simple rollover logic simulation: assume 5 digits meter
                    diff = diff.add(new BigDecimal("100000.00"));
                }
                sanLuongTho = diff.multiply(target.heSoNhan);

                long generatedId = Math.abs((target.meterId + "_" + target.bcs + "_" + month + "_" + period + "_" + target.maCto).hashCode());

                // Perform quality validation
                String status = "VALIDATED";
                String reason = null;

                if (end.compareTo(start) < 0 && !coQuayVong) {
                    status = "PENDING_MANUAL";
                    reason = "Index value dropped without hardware rollover capability.";
                } else if (sanLuongTho.compareTo(new BigDecimal("5000.00")) > 0) {
                    status = "SUSPECT";
                    reason = "Consumption spike warnings (exceeds 5000 kWh limit).";
                }

                // Check schedule tolerance window
                LocalDate readingDate = ngayDoc.toLocalDateTime().toLocalDate();
                if ("VALIDATED".equals(status)) {
                    if (readingDate.isBefore(minDate) || readingDate.isAfter(maxDate)) {
                        status = "PENDING_MANUAL";
                        reason = String.format("Reading date (%s) outside tolerance window [N-N=%s, N+N=%s] compared to target date (%s)",
                                readingDate, minDate, maxDate, targetDate);
                    }
                }

                // Publish exception event
                if (!"VALIDATED".equals(status)) {
                    try {
                        Map<String, Object> validationError = new HashMap<>();
                        validationError.put("accountId", target.accountId);
                        validationError.put("meterPointId", target.meterId);
                        validationError.put("bcs", target.bcs);
                        validationError.put("billingCycleMonth", month + "_" + period);
                        validationError.put("status", status);
                        validationError.put("reason", reason);
                        validationError.put("startIndex", start);
                        validationError.put("endIndex", end);
                        validationError.put("timestamp", LocalDateTime.now().toString());

                        kafkaTemplate.send("meter-reading-validation-results", target.accountId, objectMapper.writeValueAsString(validationError));
                    } catch (Exception e) {
                        log.error("Failed to publish validation error: {}", e.getMessage());
                    }
                }

                batchInsertParams.add(new Object[] {
                        generatedId,
                        1, // lan_doc_phu
                        target.accountId,
                        target.meterId,
                        month,
                        period,
                        Timestamp.valueOf(readingDate.minusDays(30).atStartOfDay()), // dummy tu_ngay
                        ngayDoc,
                        start,
                        end,
                        coQuayVong,
                        sanLuongTho,
                        status,
                        target.bcs, // tgian_bdien
                        target.maCto // ma_cto
                });

                processedAccounts.add(target.accountId);

                // Set processed cache in Redis
                String redisKey = "processed:meters:" + month + ":" + period;
                redisTemplate.opsForSet().add(redisKey, target.meterId + ":" + target.bcs + ":" + target.maCto);
                redisTemplate.expire(redisKey, 30, java.util.concurrent.TimeUnit.DAYS);
            }

            // Write back to PostgreSQL via Repository Bulk Insert
            if (!batchInsertParams.isEmpty()) {
                amrIngestionRepository.batchInsertAmrReadings(batchInsertParams);
                log.info("[ORACLE-AMR-JOB] Batch inserted {} readings into Postgres.", batchInsertParams.size());

                // Trigger Completeness and Calculation for each unique processed account
                for (String accId : processedAccounts) {
                    String indicatorMeter = chunk.stream()
                            .filter(t -> t.accountId.equals(accId))
                            .map(t -> t.meterId)
                            .findFirst()
                            .orElse("UNKNOWN");
                    cmisIngestionListener.checkAndTriggerBilling(accId, month, period, indicatorMeter, System.currentTimeMillis());
                }
            }

        } catch (Exception e) {
            log.error("[ORACLE-AMR-JOB] Failed to process chunk of size: {}, error: {}", chunk.size(), e.getMessage(), e);
        }
    }
}
