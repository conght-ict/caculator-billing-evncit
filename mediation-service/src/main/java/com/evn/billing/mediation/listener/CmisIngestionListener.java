package com.evn.billing.mediation.listener;

import com.evn.billing.common.domain.BillingAccountSnapshot;
import com.evn.billing.common.domain.MeterUsage;
import com.evn.billing.common.domain.MeterUsageId;
import com.evn.billing.common.dto.BillingConfigSnapshot;
import com.evn.billing.common.dto.MeterPointNode;
import com.evn.billing.common.dto.BillingTaskDto;
import com.evn.billing.common.dto.MeterReadingDto;
import com.evn.billing.mediation.dto.CmisReadingEvent;
import java.util.stream.Collectors;
import com.evn.billing.mediation.repository.BillingAccountSnapshotRepository;
import com.evn.billing.mediation.repository.MeterUsageRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Header;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import java.time.LocalDateTime;
import java.time.LocalDate;
import java.util.*;
import java.nio.ByteBuffer;
import java.math.BigDecimal;

@Component
public class CmisIngestionListener {

    private static final Logger log = LoggerFactory.getLogger(CmisIngestionListener.class);

    @Autowired
    private MeterUsageRepository meterUsageRepository;

    @Autowired
    private BillingAccountSnapshotRepository snapshotRepository;

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private org.springframework.data.redis.core.StringRedisTemplate redisTemplate;

    @Autowired
    private com.evn.billing.mediation.repository.AmrIngestionRepository amrIngestionRepository;
 
    @Autowired
    private com.evn.billing.mediation.repository.BillInvoiceRepository billInvoiceRepository;

    @Autowired
    private com.evn.billing.mediation.validation.ReadingsValidationEngine validationEngine;

    @Autowired
    private com.evn.billing.mediation.validation.SolarMeterBcsAdapter solarMeterBcsAdapter;

    /**
     * Helper to load account snapshot profile (Cache-aside using Redis & Postgres).
     */
    public BillingConfigSnapshot getSnapshotConfig(String maKhang, String month, int period) {
        String cacheKey = "snapshot:" + maKhang + ":" + month + ":" + period;
        BillingConfigSnapshot config = null;
        try {
            String cachedJson = redisTemplate.opsForValue().get(cacheKey);
            if (cachedJson != null) {
                config = objectMapper.readValue(cachedJson, BillingConfigSnapshot.class);
            }
        } catch (Exception e) {
            log.error("Failed to read snapshot from Redis: {}", e.getMessage());
        }

        if (config == null) {
            Optional<BillingAccountSnapshot> snapshotOpt = snapshotRepository
                    .findByMaKhangAndThangChuKyAndKyChotAndPhienBanTinh(maKhang, month, period, 1);
            if (snapshotOpt.isPresent()) {
                config = snapshotOpt.get().getDuLieuCauHinh();
                try {
                    redisTemplate.opsForValue().set(cacheKey, objectMapper.writeValueAsString(config), 24, java.util.concurrent.TimeUnit.HOURS);
                } catch (Exception e) {
                    // Ignore
                }
            }
        }
        return config;
    }

    /**
     * Consumes index reading events from CMIS via Kafka in batches.
     * Handles both daily telemetry reading and final chốt kỳ index readings.
     */
    @KafkaListener(
            topics = "meter-readings-input",
            groupId = "mediation-group",
            containerFactory = "kafkaBatchListenerContainerFactory"
    )
    @Transactional
    public void listenCmisReadingBatch(
            java.util.List<org.apache.kafka.clients.consumer.ConsumerRecord<String, CmisReadingEvent>> records) {
        if (records == null || records.isEmpty()) return;
        
        log.info("Mediation batch listener received size: {}", records.size());

        // Batch load all configs and initialize schedule cache to prevent N+1 queries
        Map<String, BillingConfigSnapshot> localConfigCache = batchLoadConfigs(records);
        Map<String, List<Map<String, Object>>> scheduleCache = new HashMap<>();

        // No longer querying meter_model since it is deleted. Rollover limits are processed dynamically.

        List<Object[]> meterUsageBatch = new ArrayList<>();
        
        for (org.apache.kafka.clients.consumer.ConsumerRecord<String, CmisReadingEvent> record : records) {
            CmisReadingEvent event = record.value();
            if (event == null) continue;

            String rawMonth = event.getThangChuKy();
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
            String eventJson = null;
            try {
                eventJson = objectMapper.writeValueAsString(event);
            } catch (Exception e) {}
            amrIngestionRepository.logIngestionLifecycle(event.getMaKhang(), event.getMaDdo(), month, period, "INGESTION", "RECEIVED", eventJson, event.getNguonGhi());

            long generatedId = Math.abs((event.getMaDdo() + "_" + month + "_" + period).hashCode());
            
            boolean indexDropped = event.getChiSoCuoi().compareTo(event.getChiSoDau()) < 0;
            boolean isRollover = indexDropped && event.getSoLanQuayVong() != null && event.getSoLanQuayVong() > 0;
            BigDecimal rawConsumption;
            int rCount = event.getSoLanQuayVong() != null ? event.getSoLanQuayVong() : 1;
            if (isRollover) {
                double startVal = event.getChiSoDau().doubleValue();
                double digits = Math.ceil(Math.log10(startVal));
                if (digits <= 0) digits = 5;
                BigDecimal maxVal = BigDecimal.valueOf(Math.pow(10, digits));
                rawConsumption = maxVal.multiply(BigDecimal.valueOf(rCount)).subtract(event.getChiSoDau()).add(event.getChiSoCuoi());
            } else if (!indexDropped) {
                rawConsumption = event.getChiSoCuoi().subtract(event.getChiSoDau());
            } else {
                // Index dropped without declared rollover — treat as zero pending manual review
                rawConsumption = BigDecimal.ZERO;
            }

            LocalDateTime fromDate = event.getTuNgay() != null ? event.getTuNgay() : LocalDateTime.now().minusDays(30);
            LocalDateTime toDate = event.getDenNgay() != null ? event.getDenNgay() : LocalDateTime.now();

            String status = "VALIDATED";
            String reason = null;

            // Perform quality validation checks
            if (indexDropped && !isRollover) {
                status = "PENDING_MANUAL";
                reason = "Index value dropped without hardware rollover capability.";
            } else if (rawConsumption.compareTo(new BigDecimal("5000.00")) > 0) {
                status = "SUSPECT";
                reason = "Consumption spike warnings (exceeds 5000 kWh limit).";
            }            // [Nghiệp vụ Phân biệt Chỉ số đo xa hàng ngày và Chỉ số chốt kỳ cước]
            BillingConfigSnapshot config = localConfigCache.get(event.getMaKhang() + ":" + month + ":" + period);
            if ("VALIDATED".equals(status)) {
                if (config != null && config.getDenNgay() != null && event.getDenNgay() != null) {
                    // Nếu thời điểm đọc nhỏ hơn ngày chốt kỳ cước đã cấu hình -> chỉ ghi nhận làm chỉ số đo xa đo đếm
                    if (event.getDenNgay().toLocalDate().isBefore(config.getDenNgay())) {
                        status = "TELEMETRY"; // Chỉ số đo đếm hàng ngày
                    }
                }
            }

            Map<String, Object> valDetail = new HashMap<>();
            valDetail.put("reason", reason);
            valDetail.put("consumption", rawConsumption);
            valDetail.put("startIndex", event.getChiSoDau());
            valDetail.put("endIndex", event.getChiSoCuoi());
            String valDetailJson = null;
            try {
                valDetailJson = objectMapper.writeValueAsString(valDetail);
            } catch (Exception e) {}
            amrIngestionRepository.logIngestionLifecycle(event.getMaKhang(), event.getMaDdo(), month, period, "VALIDATION", status, valDetailJson, event.getNguonGhi());

            // [Nghiệp vụ kiểm tra dung sai ngày N-1 và N+1 đối với đối tượng quản lý]
            // Rule này chỉ áp dụng cho chỉ số chốt kỳ cước (được định danh là VALIDATED) của Đối tượng quản lý
            if ("VALIDATED".equals(status)) {
                try {
                    String dtuongQly = config != null ? config.getDtuongQly() : null;
                    
                    if (dtuongQly != null && !dtuongQly.isEmpty()) {
                        String schedKey = dtuongQly + ":" + month + ":" + period;
                        List<Map<String, Object>> schedules = scheduleCache.get(schedKey);
                        if (schedules == null) {
                            schedules = amrIngestionRepository.findScheduleTolerance(dtuongQly, month, period);
                            scheduleCache.put(schedKey, schedules);
                        }
                        
                        if (!schedules.isEmpty()) {
                            Map<String, Object> sched = schedules.get(0);
                            LocalDate denNgay = null;
                            Object denNgayObj = sched.get("den_ngay");
                            if (denNgayObj instanceof java.sql.Date) {
                                denNgay = ((java.sql.Date) denNgayObj).toLocalDate();
                            } else if (denNgayObj instanceof LocalDate) {
                                denNgay = (LocalDate) denNgayObj;
                            }
                            
                            Number nTruNum = (Number) sched.get("n_tru");
                            Number nCongNum = (Number) sched.get("n_cong");
                            int nTru = nTruNum != null ? nTruNum.intValue() : 0;
                            int nCong = nCongNum != null ? nCongNum.intValue() : 0;
                            
                            if (denNgay != null && event.getDenNgay() != null) {
                                LocalDate readingDate = event.getDenNgay().toLocalDate();
                                LocalDate minAllowed = denNgay.minusDays(nTru);
                                LocalDate maxAllowed = denNgay.plusDays(nCong);
                                
                                if (readingDate.isBefore(minAllowed) || readingDate.isAfter(maxAllowed)) {
                                    status = "PENDING_MANUAL";
                                    reason = String.format("Reading date (%s) outside tolerance window [N-%d=%s, N+%d=%s] compared to target date (%s)",
                                            readingDate, nTru, minAllowed, nCong, maxAllowed, denNgay);
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    log.warn("[VALIDATION] Failed to check schedule tolerance for account: {}, error: {}", event.getMaKhang(), e.getMessage());
                }
            }

            // Publish exception event to CMIS validation results topic if validation fails
            if (!"VALIDATED".equals(status) && !"TELEMETRY".equals(status)) {
                try {
                    Map<String, Object> validationError = new HashMap<>();
                    if (config == null || config.getDtuongQly() == null) {
                        throw new IllegalStateException("Snapshot configuration or dtuongQly is missing for account: " + event.getMaKhang());
                    }
                    String dtuongQly = config.getDtuongQly();
                    validationError.put("dtuongQly", dtuongQly);
                    validationError.put("maKhang", event.getMaKhang());
                    validationError.put("meterPointId", event.getMaDdo());
                    validationError.put("billingCycleMonth", event.getThangChuKy());
                    validationError.put("usageId", generatedId);
                    validationError.put("status", status);
                    validationError.put("reason", reason);
                    validationError.put("startIndex", event.getChiSoDau());
                    validationError.put("endIndex", event.getChiSoCuoi());
                    validationError.put("timestamp", LocalDateTime.now().toString());

                    String validationErrorJson = objectMapper.writeValueAsString(validationError);
                    kafkaTemplate.send("meter-reading-validation-results", event.getMaKhang(), validationErrorJson);
                    log.warn("[VALIDATION] Flagged anomalous reading for Account: {}, Status: {}, Reason: {}",
                            event.getMaKhang(), status, reason);
 
                    // Write validation error log to nhat_ky_chi_so
                    amrIngestionRepository.logIngestionLifecycle(
                        event.getMaKhang(), event.getMaDdo(), month, period, 
                        "VALIDATION", status, validationErrorJson, event.getNguonGhi()
                    );
                    
                    // Update customer billing status to SUSPECT or PENDING_MANUAL
                    amrIngestionRepository.updateCustomerBillingStatus(
                        event.getMaKhang(), month, period, status, reason
                    );
                } catch (Exception e) {
                    log.error("Failed to publish validation error event to Kafka or DB: {}", e.getMessage());
                }
            }

            String rawBcs = event.getTgianBdien() != null ? event.getTgianBdien() : "BT";
            boolean isDienMt = isDienMtMeterPoint(config, event.getMaDdo());
            String adaptedBcs = solarMeterBcsAdapter.adaptBcs(rawBcs, isDienMt);

            meterUsageBatch.add(new Object[] {
                generatedId,
                1, // lan_doc_phu
                event.getMaKhang(),
                event.getMaDdo(),
                month,
                period,
                java.sql.Timestamp.valueOf(fromDate),
                java.sql.Timestamp.valueOf(toDate),
                event.getChiSoDau(),
                event.getChiSoCuoi(),
                isRollover,
                rawConsumption,
                status,
                event.getNguonGhi() != null ? event.getNguonGhi() : "AMR",
                adaptedBcs,
                event.getMaCto() != null ? event.getMaCto() : "UNKNOWN",
                rCount
            });
        }

        // 1. Bulk insert usages using Repository Bulk Insert (ON CONFLICT DO NOTHING for strict Deduplication)
        if (!meterUsageBatch.isEmpty()) {
            amrIngestionRepository.batchInsertCmisReadings(meterUsageBatch);
            log.info("Ingested batch of {} readings to DB via JDBC Batch (Deduplicated).", meterUsageBatch.size());
        }

        // 2. Perform completeness check and trigger calculation task only for final billing index readings
        for (org.apache.kafka.clients.consumer.ConsumerRecord<String, CmisReadingEvent> record : records) {
            CmisReadingEvent event = record.value();
            if (event == null) continue;

            String rawMonth = event.getThangChuKy();
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

            // Check if it is the final billing index of the period
            BillingConfigSnapshot config = localConfigCache.get(event.getMaKhang() + ":" + month + ":" + period);
            boolean isBillingReading = true;
            if (config != null && config.getDenNgay() != null && event.getDenNgay() != null) {
                if (event.getDenNgay().toLocalDate().isBefore(config.getDenNgay())) {
                    isBillingReading = false;
                }
            }

            if (isBillingReading) {
                long t1Ingest = System.currentTimeMillis();
                org.apache.kafka.common.header.Header h = record.headers().lastHeader("t1_ingest");
                if (h != null) {
                    try {
                        t1Ingest = ByteBuffer.wrap(h.value()).getLong();
                    } catch (Exception e) {
                        // Ignore
                    }
                }
                checkAndTriggerBilling(event.getMaKhang(), month, period, event.getMaDdo(), t1Ingest, config);
            }
        }
    }

    private void collectRequiredReadings(MeterPointNode node, Set<String> requiredReadings) {
        List<com.evn.billing.common.dto.MeterDetails> activeCtos = node.getActiveMeters();
        if (activeCtos != null && !activeCtos.isEmpty()) {
            for (com.evn.billing.common.dto.MeterDetails cto : activeCtos) {
                List<String> bcsList = cto.getDanhSachBcs();
                if (bcsList == null || bcsList.isEmpty()) {
                    bcsList = List.of("KT");
                }
                String maCto = cto.getMaCto();
                if (maCto == null) maCto = cto.getSoSeri();
                if (maCto == null) maCto = "UNKNOWN";

                for (String bcs : bcsList) {
                    requiredReadings.add(node.getMaDdo() + ":" + bcs + ":" + maCto);
                }
            }
        } else {
            // Fallback to legacy behavior if activeMeters is empty
            if (node.getPriceRules() != null && !node.getPriceRules().isEmpty()) {
                for (com.evn.billing.common.dto.PriceApplicationRule rule : node.getPriceRules()) {
                    String bcs = rule.getTgianBdien() != null ? rule.getTgianBdien() : "BT";
                    requiredReadings.add(node.getMaDdo() + ":" + bcs + ":UNKNOWN");
                }
            } else {
                requiredReadings.add(node.getMaDdo() + ":BT:UNKNOWN");
            }
        }

        if (node.getChildPoints() != null) {
            for (MeterPointNode child : node.getChildPoints()) {
                collectRequiredReadings(child, requiredReadings);
            }
        }
    }

    public void checkAndTriggerBilling(String maKhang, String month, int period, String currentMeterPointId, long t1Ingest) {
        BillingConfigSnapshot config = getSnapshotConfig(maKhang, month, period);
        checkAndTriggerBilling(maKhang, month, period, currentMeterPointId, t1Ingest, config);
    }

    public void checkAndTriggerBilling(String maKhang, String month, int period, String currentMeterPointId, long t1Ingest, BillingConfigSnapshot config) {
        log.info("[INGESTION] Triggering billing check for Account: {}, Month: {}, Period: {}", maKhang, month, period);
        
        if (config == null) {
            log.warn("[INGESTION] Snapshot missing for Account: {}, Month: {}, Period: {}", maKhang, month, period);
            return;
        }

        if (config.getDtuongQly() == null) {
            throw new IllegalStateException("Snapshot configuration is missing dtuongQly for account: " + maKhang);
        }
        String dtuongQly = config.getDtuongQly();
 
        if (amrIngestionRepository.isBatchJobRunning(dtuongQly, month, period)) {
            log.info("[INGESTION] Spring Batch Job is running for Book: {}, Month: {}, Period: {}. Skipping ROLLING trigger.", 
                    dtuongQly, month, period);
            return;
        }
 
        if (config.getMeterTopology() == null || config.getMeterTopology().getRootPoints() == null) {
            log.warn("[INGESTION] Topology is null or root points null for Account: {}", maKhang);
            return;
        }

        // Collect all required readings (meterPointId + ":" + bcs + ":" + maCto) from the topology
        Set<String> requiredReadings = new HashSet<>();
        for (MeterPointNode root : config.getMeterTopology().getRootPoints()) {
            collectRequiredReadings(root, requiredReadings);
        }

        // 3. Completeness check
        boolean isComplete = false;
        Set<String> receivedReadings = new HashSet<>();
        
        // Fetch database for all validated readings of this account, month, period
        List<MeterUsage> validatedUsages = meterUsageRepository.findByMaKhangAndThangChuKyAndKyChotAndTrangThaiXuLy(maKhang, month, period, "VALIDATED");
        
        // Filter validated usages to only keep the latest subReadingSeq for each meter register
        Map<String, MeterUsage> latestUsagesMap = new HashMap<>();
        for (MeterUsage u : validatedUsages) {
            String maCto = u.getMaCto();
            if (maCto == null) maCto = "UNKNOWN";
            String key = u.getMaDdo() + ":" + u.getTgianBdien() + ":" + maCto;
            MeterUsage existing = latestUsagesMap.get(key);
            if (existing == null || u.getLanDocPhu() > existing.getLanDocPhu()) {
                latestUsagesMap.put(key, u);
            }
        }
        
        for (String key : latestUsagesMap.keySet()) {
            receivedReadings.add(key);
        }
        
        if (receivedReadings.containsAll(requiredReadings)) {
            isComplete = true;
        }

        log.info("[AUDIT-TRACER] [Account: {}] Step 3: Topology readiness check. Required registers: {}, Received registers: {}.", maKhang, requiredReadings, receivedReadings);

        if (isComplete) {
            // Log completeness success
            amrIngestionRepository.logIngestionLifecycle(maKhang, null, month, period, "COMPLETENESS", "COMPLETE", null, "SYSTEM");

            // Run validation engine rules (Pmax, CSPK, Abnormal Spike) before triggering billing
            com.evn.billing.mediation.validation.ValidationResult valResult = validationEngine.validate(maKhang, month, period, config, validatedUsages);
            if (!valResult.isValid()) {
                String errorMsg = String.join("; ", valResult.getErrors());
                log.warn("[INGESTION] Account {} failed validation check: {}. Updating status to PENDING_MANUAL.", maKhang, errorMsg);
                amrIngestionRepository.updateCustomerBillingStatus(maKhang, month, period, valResult.getStatus(), errorMsg);
                
                String errorsJson = null;
                try {
                    errorsJson = objectMapper.writeValueAsString(valResult.getErrors());
                } catch (Exception e) {}
                amrIngestionRepository.logIngestionLifecycle(maKhang, null, month, period, "VALIDATION", valResult.getStatus(), errorsJson, "SYSTEM");
                return;
            }

            // Log validation success
            amrIngestionRepository.logIngestionLifecycle(maKhang, null, month, period, "VALIDATION", "PASS", null, "SYSTEM");

            if (!tryAcquireBillingTriggerGate(dtuongQly, maKhang, month, period)) {
                log.info("[INGESTION] Skip duplicate trigger for Account: {}, Month: {}, Period: {} because status gate is already claimed.", maKhang, month, period);
                return;
            }

            log.info("[AUDIT-TRACER] [Account: {}] Step 3.1: Netting readiness verified. Triggering calculation task via Kafka.", maKhang);
            
            // Send billing task to Kafka
            BillingTaskDto task = new BillingTaskDto();
            task.setMaKhang(maKhang);
            task.setDtuongQly(dtuongQly);
            task.setThangChuKy(month);
            task.setKyChot(period);
            int nextVersion = (int) billInvoiceRepository.countByMaKhangAndThangChuKyAndKyChot(maKhang, month, period) + 1;
            task.setPhienBanTinh(nextVersion);
            task.setTraceId(UUID.randomUUID().toString().replace("-", ""));
            task.setTriggeredBy("ROLLING");

            // Resolve classification flags from snapshot & usages
            String finalChangeFlag = "NONE";
            if (config != null && config.getChangeFlags() != null) {
                finalChangeFlag = config.getChangeFlags();
            }
            boolean hasReadingChange = validatedUsages.stream().anyMatch(u -> "CORRECTION".equals(u.getLoaiGhiIndex()) || (u.getLanDocPhu() != null && u.getLanDocPhu() > 1));
            if (hasReadingChange) {
                if ("NONE".equals(finalChangeFlag)) {
                    finalChangeFlag = "READING_CHANGE";
                } else {
                    finalChangeFlag = "MULTI_CHANGE";
                }
            }
            task.setChangeFlags(finalChangeFlag);
            task.setLoaiKhang(config != null ? config.getCustomerType() : "SINH_HOAT");
            task.setHasRelation(config != null && config.isHasRelation());
            
            List<MeterReadingDto> readings = latestUsagesMap.values().stream()
                .map(u -> new MeterReadingDto(
                    u.getMaDdo(),
                    u.getTuNgay(),
                    u.getDenNgay(),
                    u.getChiSoDau(),
                    u.getChiSoCuoi(),
                    u.getConsumption(),
                    u.getCoQuayVong(),
                    u.getMaxRegisterSnapshot(),
                    u.getLanDocPhu(),
                    u.getLoaiGhiIndex(),
                    u.getMaCto(),
                    u.getSoLanQuayVong(),
                    u.getTgianBdien()
                ))
                .collect(Collectors.toList());
            task.setDanhSachChiSo(readings);

            long t2Send = System.currentTimeMillis();
            ProducerRecord<String, Object> producerRecord = new ProducerRecord<>("billing-execution-topic", maKhang, task);
            producerRecord.headers().add("t1_ingest", ByteBuffer.allocate(8).putLong(t1Ingest).array());
            producerRecord.headers().add("t2_send", ByteBuffer.allocate(8).putLong(t2Send).array());

            kafkaTemplate.send(producerRecord);
            log.info("[INGESTION] Successfully sent billing task to billing-execution-topic for Account: {}", maKhang);
        } else {
            log.info("[INGESTION] Account {} is missing readings. Required: {}, Received: {}", maKhang, requiredReadings, receivedReadings);
            
            // Log missing registers into trang_thai_tinh_toan_kh to allow CMIS/Portal queries
            try {
                Set<String> missing = new HashSet<>(requiredReadings);
                missing.removeAll(receivedReadings);
                
                Map<String, Object> errorMap = new HashMap<>();
                errorMap.put("error_type", "INCOMPLETE_READINGS");
                errorMap.put("missing_registers", missing);
                String missingStr = objectMapper.writeValueAsString(errorMap);
 
                amrIngestionRepository.logIncompleteStatus(maKhang, month, dtuongQly, period, missingStr);
                log.info("[INGESTION] Logged INCOMPLETE status for Account: {} due to: {}", maKhang, missingStr);

                Map<String, Object> compDetail = new HashMap<>();
                compDetail.put("required", requiredReadings);
                compDetail.put("received", receivedReadings);
                compDetail.put("missing", missing);
                String compDetailJson = null;
                try {
                    compDetailJson = objectMapper.writeValueAsString(compDetail);
                } catch (Exception e) {}
                amrIngestionRepository.logIngestionLifecycle(maKhang, null, month, period, "COMPLETENESS", "INCOMPLETE", compDetailJson, "SYSTEM");
            } catch (Exception e) {
                log.error("[INGESTION] Failed to log INCOMPLETE status for Account: {}", maKhang, e);
            }
        }
    }

    private boolean tryAcquireBillingTriggerGate(String dtuongQly, String maKhang, String month, int period) {
        return amrIngestionRepository.tryAcquireBillingTriggerGate(dtuongQly, maKhang, month, period);
    }

    public Map<String, BillingConfigSnapshot> batchLoadConfigs(
            List<org.apache.kafka.clients.consumer.ConsumerRecord<String, CmisReadingEvent>> records) {
        
        Map<String, BillingConfigSnapshot> cache = new HashMap<>();
        if (records == null || records.isEmpty()) return cache;

        // Group unique accounts by month and period
        Map<String, Set<String>> groupToAccounts = new HashMap<>(); // key: "month:period" -> set of accountIds
        for (org.apache.kafka.clients.consumer.ConsumerRecord<String, CmisReadingEvent> record : records) {
            CmisReadingEvent event = record.value();
            if (event == null) continue;

            String rawMonth = event.getThangChuKy();
            String month = rawMonth;
            int period = 1;
            if (rawMonth != null && rawMonth.contains("_")) {
                int lastUnderscore = rawMonth.lastIndexOf('_');
                try {
                    period = Integer.parseInt(rawMonth.substring(lastUnderscore + 1));
                    month = rawMonth.substring(0, lastUnderscore);
                } catch (NumberFormatException e) {
                    // Ignore
                }
            }
            String key = month + ":" + period;
            groupToAccounts.computeIfAbsent(key, k -> new HashSet<>()).add(event.getMaKhang());
        }

        // Batch load all configurations
        for (Map.Entry<String, Set<String>> entry : groupToAccounts.entrySet()) {
            String[] parts = entry.getKey().split(":");
            String month = parts[0];
            int period = Integer.parseInt(parts[1]);
            List<String> accountIds = new ArrayList<>(entry.getValue());

            Map<String, BillingConfigSnapshot> configs = getSnapshotConfigs(accountIds, month, period);
            for (Map.Entry<String, BillingConfigSnapshot> configEntry : configs.entrySet()) {
                String accId = configEntry.getKey();
                cache.put(accId + ":" + month + ":" + period, configEntry.getValue());
            }
        }
        return cache;
    }

    public Map<String, BillingConfigSnapshot> getSnapshotConfigs(List<String> accountIds, String month, int period) {
        Map<String, BillingConfigSnapshot> result = new HashMap<>();
        if (accountIds == null || accountIds.isEmpty()) return result;

        List<String> cacheKeys = new ArrayList<>();
        Map<String, String> keyToAccountId = new HashMap<>();
        for (String maKhang : accountIds) {
            String key = "snapshot:" + maKhang + ":" + month + ":" + period;
            cacheKeys.add(key);
            keyToAccountId.put(key, maKhang);
        }

        List<String> cachedJsons = null;
        try {
            cachedJsons = redisTemplate.opsForValue().multiGet(cacheKeys);
        } catch (Exception e) {
            log.error("Failed to batch read snapshots from Redis: {}", e.getMessage());
        }

        List<String> missingAccountIds = new ArrayList<>();
        if (cachedJsons != null) {
            for (int i = 0; i < cacheKeys.size(); i++) {
                String key = cacheKeys.get(i);
                String json = cachedJsons.get(i);
                String maKhang = keyToAccountId.get(key);
                if (json != null) {
                    try {
                        BillingConfigSnapshot config = objectMapper.readValue(json, BillingConfigSnapshot.class);
                        result.put(maKhang, config);
                    } catch (Exception e) {
                        log.error("Failed to parse cached snapshot for account: {}, error: {}", maKhang, e.getMessage());
                        missingAccountIds.add(maKhang);
                    }
                } else {
                    missingAccountIds.add(maKhang);
                }
            }
        } else {
            missingAccountIds.addAll(accountIds);
        }

        if (!missingAccountIds.isEmpty()) {
            // Batch load from Postgres DB using IN query
            List<BillingAccountSnapshot> snapshots = snapshotRepository.findByMaKhangInAndThangChuKyAndKyChotAndPhienBanTinh(
                    missingAccountIds, month, period, 1
            );
            Map<String, String> redisMSet = new HashMap<>();
            for (BillingAccountSnapshot snapshot : snapshots) {
                BillingConfigSnapshot config = snapshot.getDuLieuCauHinh();
                result.put(snapshot.getMaKhang(), config);
                String key = "snapshot:" + snapshot.getMaKhang() + ":" + month + ":" + period;
                try {
                    redisMSet.put(key, objectMapper.writeValueAsString(config));
                } catch (Exception e) {
                    // Ignore
                }
            }
            if (!redisMSet.isEmpty()) {
                try {
                    redisTemplate.opsForValue().multiSet(redisMSet);
                    // Set expiration for each key in batch
                    for (String key : redisMSet.keySet()) {
                        redisTemplate.expire(key, 24, java.util.concurrent.TimeUnit.HOURS);
                    }
                } catch (Exception e) {
                    // Ignore
                }
            }
        }

        return result;
    }

    private boolean isDienMtMeterPoint(BillingConfigSnapshot config, String meterPointId) {
        if (config == null || config.getMeterTopology() == null || config.getMeterTopology().getRootPoints() == null) {
            return false;
        }
        for (com.evn.billing.common.dto.MeterPointNode node : config.getMeterTopology().getRootPoints()) {
            Boolean found = findIsDienMt(node, meterPointId);
            if (found != null) {
                return found;
            }
        }
        return false;
    }

    private Boolean findIsDienMt(com.evn.billing.common.dto.MeterPointNode node, String meterPointId) {
        if (node.getMaDdo().equals(meterPointId)) {
            return node.getIsDienMt() != null ? node.getIsDienMt() : false;
        }
        if (node.getChildPoints() != null) {
            for (com.evn.billing.common.dto.MeterPointNode child : node.getChildPoints()) {
                Boolean found = findIsDienMt(child, meterPointId);
                if (found != null) {
                    return found;
                }
            }
        }
        return null;
    }
}
