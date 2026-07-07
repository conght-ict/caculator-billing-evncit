package com.evn.billing.mediation.listener;

import com.evn.billing.common.domain.MeterUsage;
import com.evn.billing.common.domain.MeterUsageId;
import com.evn.billing.mediation.dto.ReadingResolutionEvent;
import com.evn.billing.mediation.repository.MeterUsageRepository;
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

@Component
public class ReadingResolutionListener {

    private static final Logger log = LoggerFactory.getLogger(ReadingResolutionListener.class);

    @Autowired
    private MeterUsageRepository meterUsageRepository;

    @Autowired
    private CmisIngestionListener cmisIngestionListener;

    @Autowired
    private ObjectMapper objectMapper;

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

            if ("ACCEPT_AS_IS".equalsIgnoreCase(event.getResolutionType())) {
                processAcceptAsIs(event);
            } else if ("CORRECT".equalsIgnoreCase(event.getResolutionType())) {
                processCorrection(event);
            } else {
                log.warn("Unknown resolutionType: {}", event.getResolutionType());
            }
        } catch (Exception e) {
            log.error("Failed to process resolution event: {}", e.getMessage(), e);
        }
    }

    private void processAcceptAsIs(ReadingResolutionEvent event) {
        log.info("[RESOLUTION] Accept As-Is command for Account: {}, Usage ID: {}", event.getAccountId(), event.getUsageId());
        
        String rawMonth = event.getBillingCycleMonth();
        String month = rawMonth;
        int period = 1;
        if (rawMonth != null && rawMonth.contains("_")) {
            int lastUnderscore = rawMonth.lastIndexOf('_');
            String suffix = rawMonth.substring(lastUnderscore + 1);
            try {
                period = Integer.parseInt(suffix);
                month = rawMonth.substring(0, lastUnderscore);
            } catch (NumberFormatException e) {
                // Ignore
            }
        }

        // Find original record (subReadingSeq = 1)
        MeterUsageId originalKey = new MeterUsageId(event.getUsageId(), 1, month, period);
        Optional<MeterUsage> originalOpt = meterUsageRepository.findById(originalKey);

        if (originalOpt.isEmpty()) {
            log.error("[RESOLUTION] Original usage record not found for Key: {}", originalKey);
            return;
        }

        MeterUsage original = originalOpt.get();
        original.setStatus("VALIDATED"); // Approve status to validated (indices remain unchanged)
        meterUsageRepository.save(original);

        // Trigger billing calculation checking
        long t1Ingest = System.currentTimeMillis();
        cmisIngestionListener.checkAndTriggerBilling(original.getAccountId(), original.getBillingCycleMonth(), original.getPeriod(), original.getMeterPointId(), t1Ingest);
        log.info("[RESOLUTION] Successfully accepted suspect reading and triggered billing check.");
    }

    private void processCorrection(ReadingResolutionEvent event) {
        log.info("[RESOLUTION] Correct reading command for Account: {}, Usage ID: {}, Corrected End Index: {}",
                event.getAccountId(), event.getUsageId(), event.getCorrectedEndIndex());

        String rawMonth = event.getBillingCycleMonth();
        String month = rawMonth;
        int period = 1;
        if (rawMonth != null && rawMonth.contains("_")) {
            int lastUnderscore = rawMonth.lastIndexOf('_');
            String suffix = rawMonth.substring(lastUnderscore + 1);
            try {
                period = Integer.parseInt(suffix);
                month = rawMonth.substring(0, lastUnderscore);
            } catch (NumberFormatException e) {
                // Ignore
            }
        }

        // Find original record (subReadingSeq = 1)
        MeterUsageId originalKey = new MeterUsageId(event.getUsageId(), 1, month, period);
        Optional<MeterUsage> originalOpt = meterUsageRepository.findById(originalKey);

        if (originalOpt.isEmpty()) {
            log.error("[RESOLUTION] Original usage record not found for Key: {}", originalKey);
            return;
        }

        MeterUsage original = originalOpt.get();

        // Check if correction seq=2 already exists
        MeterUsageId correctionKey = new MeterUsageId(original.getUsageId(), 2, month, period);
        if (meterUsageRepository.existsById(correctionKey)) {
            log.warn("[RESOLUTION] Correction record seq=2 already exists for Key: {}", correctionKey);
            return;
        }

        // [I.1] Append-Only: CREATE new correction record instead of updating the original thô
        MeterUsage correction = new MeterUsage();
        correction.setUsageId(original.getUsageId());
        correction.setSubReadingSeq(2); // Sequence version 2 for correction
        correction.setBillingCycleMonth(month);
        correction.setPeriod(period);
        correction.setAccountId(original.getAccountId());
        correction.setMeterPointId(original.getMeterPointId());
        correction.setFromDate(original.getFromDate());
        correction.setToDate(original.getToDate());
        correction.setStartIndex(original.getStartIndex());
        correction.setEndIndex(event.getCorrectedEndIndex());

        // Recalculate rollover logic for corrected index
        BigDecimal maxVal = original.getMaxRegisterSnapshot();
        if (maxVal == null) {
            maxVal = new BigDecimal("99999.9");
        }
        correction.setMaxRegisterSnapshot(maxVal);

        boolean isRollover = event.getCorrectedEndIndex().compareTo(original.getStartIndex()) < 0;
        correction.setIsRollover(isRollover);
        if (isRollover) {
            correction.setRawConsumption(maxVal.subtract(original.getStartIndex()).add(event.getCorrectedEndIndex()));
        } else {
            correction.setRawConsumption(event.getCorrectedEndIndex().subtract(original.getStartIndex()));
        }

        correction.setStatus("VALIDATED");
        correction.setRecordType("CORRECTION");
        correction.setCorrectionOfUsageId(original.getUsageId());
        correction.setSource("MANUAL");
        correction.setCreatedAt(LocalDateTime.now());

        meterUsageRepository.save(correction);

        // Trigger billing calculation checking
        long t1Ingest = System.currentTimeMillis();
        cmisIngestionListener.checkAndTriggerBilling(original.getAccountId(), original.getBillingCycleMonth(), original.getPeriod(), original.getMeterPointId(), t1Ingest);
        log.info("[RESOLUTION] Successfully appended correction record seq=2 and triggered billing check.");
    }
}
