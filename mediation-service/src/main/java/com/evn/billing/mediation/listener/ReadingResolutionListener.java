package com.evn.billing.mediation.listener;

import com.evn.billing.common.domain.MeterUsage;
import com.evn.billing.common.domain.MeterUsageId;
import com.evn.billing.mediation.dto.ReadingResolutionEvent;
import com.evn.billing.mediation.repository.MeterUsageRepository;
import com.evn.billing.mediation.repository.ReadingResolutionRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Optional;
import org.springframework.dao.DataIntegrityViolationException;

@Component
public class ReadingResolutionListener {

    private static final Logger log = LoggerFactory.getLogger(ReadingResolutionListener.class);

    @Autowired
    private MeterUsageRepository meterUsageRepository;

    @Autowired
    private CmisIngestionListener cmisIngestionListener;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private ReadingResolutionRepository readingResolutionRepository;

    /**
     * Consumes user exceptions resolution events from Kafka topic 'meter-reading-resolutions'.
     * Process Case 1: ACCEPT_AS_IS (approves the suspect reading without changes).
     * Process Case 2: CORRECT (appends correction record seq=2, recalculates and triggers pipeline).
     */
    @KafkaListener(
            topics = "meter-reading-resolutions",
            groupId = "resolution-group"
    )
    @Transactional
    public void listenReadingResolutions(String message) {
        log.info("Received resolution command event: {}", message);
        try {
            ReadingResolutionEvent event = objectMapper.readValue(message, ReadingResolutionEvent.class);
            if (event == null) return;

            if ("ACCEPT_AS_IS".equalsIgnoreCase(event.getLoaiXuLy())) {
                processAcceptAsIs(event);
            } else if ("CORRECT".equalsIgnoreCase(event.getLoaiXuLy())) {
                processCorrection(event);
            } else {
                log.warn("Unknown resolutionType: {}", event.getLoaiXuLy());
            }
        } catch (Exception e) {
            log.error("Failed to process resolution event: {}", e.getMessage(), e);
        }
    }

    private MeterUsage resolveOriginalUsage(ReadingResolutionEvent event) {
        String rawMonth = event.getThangChuKy();
        String month = rawMonth;
        int period = 1;

        if (month == null || month.isEmpty()) {
            try {
                String dtuongQly = event.getDtuongQly();
                if (dtuongQly == null || dtuongQly.isEmpty()) {
                    dtuongQly = readingResolutionRepository.findBookByAccountId(event.getMaKhang());
                }
                if (dtuongQly != null && !dtuongQly.isEmpty()) {
                    java.util.Optional<java.util.Map<String, Object>> scheduleOpt =
                            readingResolutionRepository.findLatestActiveScheduleByBook(dtuongQly);
                    if (scheduleOpt.isPresent()) {
                        java.util.Map<String, Object> scheduleMap = scheduleOpt.get();
                        month = (String) scheduleMap.get("thang_ck");
                        period = ((Number) scheduleMap.get("ky_chot")).intValue();
                    }
                }
            } catch (Exception e) {
                log.warn("[RESOLUTION] Failed to resolve month/period for return: {}", event.getMaKhang());
                return null;
            }
        } else if (rawMonth.contains("_")) {
            String[] tokens = rawMonth.split("_");
            if (tokens.length == 3) {
                try {
                    period = Integer.parseInt(tokens[2]);
                    month = tokens[0] + "_" + tokens[1];
                } catch (NumberFormatException e) {
                    // Keep default period when suffix is not numeric
                    month = rawMonth;
                }
            } else {
                month = rawMonth;
            }
        }

        if (month == null || month.isEmpty()) {
            log.error("[RESOLUTION] Cannot resolve billing cycle month for Account: {}", event.getMaKhang());
            return null;
        }

        Long usageId = event.getIdChiSo();
        if (usageId == null) {
            usageId = readingResolutionRepository.findSuspectOrPendingUsageId(event.getMaKhang(), month, period);
            if (usageId == null) {
                usageId = readingResolutionRepository.findAnyUsageId(event.getMaKhang(), month, period);
                if (usageId == null) {
                    log.error("[RESOLUTION] Failed to auto-resolve usageId for Account: {}", event.getMaKhang());
                }
            }
        }

        if (usageId == null) {
            log.error("[RESOLUTION] Cannot resolve usageId for Account: {}, Month: {}, Period: {}", event.getMaKhang(), month, period);
            return null;
        }

        MeterUsageId originalKey = new MeterUsageId(usageId, 1, month, period);
        return meterUsageRepository.findById(originalKey).orElse(null);
    }

    private void processAcceptAsIs(ReadingResolutionEvent event) {
        log.info("[RESOLUTION] Accept As-Is command for Account: {}, Usage ID: {}", event.getMaKhang(), event.getIdChiSo());
        
        MeterUsage original = resolveOriginalUsage(event);
        if (original == null) {
            log.error("[RESOLUTION] Original usage record not resolved for Account: {}", event.getMaKhang());
            return;
        }

        original.setTrangThaiXuLy("VALIDATED"); // Approve status to validated (indices remain unchanged)
        meterUsageRepository.save(original);

        // Trigger billing calculation checking
        long t1Ingest = System.currentTimeMillis();
        cmisIngestionListener.checkAndTriggerBilling(original.getMaKhang(), original.getThangChuKy(), original.getKyChot(), original.getMaDdo(), t1Ingest);
        log.info("[RESOLUTION] Successfully accepted suspect reading and triggered billing check.");
    }

    private void processCorrection(ReadingResolutionEvent event) {
        log.info("[RESOLUTION] Correct reading command for Account: {}, Usage ID: {}, Corrected End Index: {}",
                event.getMaKhang(), event.getIdChiSo(), event.getChiSoCuoiDieuChinh());

        if (event.getChiSoCuoiDieuChinh() == null) {
            log.error("[RESOLUTION] Corrected end index is null. Cannot apply correction for Account: {}", event.getMaKhang());
            return;
        }

        MeterUsage original = resolveOriginalUsage(event);
        if (original == null) {
            log.error("[RESOLUTION] Original usage record not resolved for Account: {}", event.getMaKhang());
            return;
        }

        // Check if correction seq=2 already exists
        MeterUsageId correctionKey = new MeterUsageId(original.getIdChiSo(), 2, original.getThangChuKy(), original.getKyChot());
        if (meterUsageRepository.existsById(correctionKey)) {
            log.warn("[RESOLUTION] Correction record seq=2 already exists for Key: {}", correctionKey);
            return;
        }

        // [I.1] Append-Only: CREATE new correction record instead of updating the original thô
        MeterUsage correction = new MeterUsage();
        correction.setIdChiSo(original.getIdChiSo());
        correction.setLanDocPhu(2); // Sequence version 2 for correction
        correction.setThangChuKy(original.getThangChuKy());
        correction.setKyChot(original.getKyChot());
        correction.setMaKhang(original.getMaKhang());
        correction.setMaDdo(original.getMaDdo());
        correction.setTuNgay(original.getTuNgay());
        correction.setDenNgay(original.getDenNgay());
        correction.setChiSoDau(original.getChiSoDau());
        correction.setChiSoCuoi(event.getChiSoCuoiDieuChinh());

        // Recalculate rollover logic for corrected index
        boolean isRollover = event.getChiSoCuoiDieuChinh().compareTo(original.getChiSoDau()) < 0;
        correction.setCoQuayVong(isRollover);
        if (isRollover) {
            double startVal = original.getChiSoDau().doubleValue();
            double digits = Math.ceil(Math.log10(startVal));
            if (digits <= 0) digits = 5;
            BigDecimal maxVal = BigDecimal.valueOf(Math.pow(10, digits));
            correction.setMaxRegisterSnapshot(maxVal);
            correction.setSanLuongTho(maxVal.subtract(original.getChiSoDau()).add(event.getChiSoCuoiDieuChinh()));
        } else {
            correction.setMaxRegisterSnapshot(new BigDecimal("99999.9"));
            correction.setSanLuongTho(event.getChiSoCuoiDieuChinh().subtract(original.getChiSoDau()));
        }

        correction.setTrangThaiXuLy("VALIDATED");
        correction.setLoaiGhiIndex("CORRECTION");
        correction.setIdChiSoDieuChinh(original.getIdChiSo());
        correction.setNguonGhi("MANUAL");
        correction.setCreatedAt(LocalDateTime.now());

        try {
            meterUsageRepository.save(correction);
            // Mark the original record as REPLACED so it will be ignored in calculations
            original.setTrangThaiXuLy("REPLACED");
            meterUsageRepository.save(original);
        } catch (DataIntegrityViolationException duplicateKey) {
            log.warn("[RESOLUTION] Correction record seq=2 already inserted concurrently for usage: {}", original.getIdChiSo());
            return;
        }

        // Trigger billing calculation checking
        long t1Ingest = System.currentTimeMillis();
        cmisIngestionListener.checkAndTriggerBilling(original.getMaKhang(), original.getThangChuKy(), original.getKyChot(), original.getMaDdo(), t1Ingest);
        log.info("[RESOLUTION] Successfully appended correction record seq=2 and triggered billing check.");
    }
}
