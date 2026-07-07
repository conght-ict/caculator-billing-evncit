package com.evn.billing.mediation.listener;

import com.evn.billing.common.domain.BillingAccountSnapshot;
import com.evn.billing.common.domain.MeterUsage;
import com.evn.billing.common.domain.MeterUsageId;
import com.evn.billing.common.dto.BillingConfigSnapshot;
import com.evn.billing.common.dto.MeterPointNode;
import com.evn.billing.mediation.dto.BillingTaskDto;
import com.evn.billing.mediation.dto.CmisReadingEvent;
import com.evn.billing.mediation.dto.MeterReadingDto;
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
    private org.springframework.jdbc.core.JdbcTemplate jdbcTemplate;

    /**
     * Helper to load account snapshot profile (Cache-aside using Redis & Postgres).
     */
    public BillingConfigSnapshot getSnapshotConfig(String accountId, String month, int period) {
        String cacheKey = "snapshot:" + accountId + ":" + month + ":" + period;
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
                    .findByAccountIdAndBillingCycleMonthAndPeriodAndCalculationVersion(accountId, month, period, 1);
            if (snapshotOpt.isPresent()) {
                config = snapshotOpt.get().getConfigData();
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

        // Extract all meter point IDs to batch query their max registers
        Set<String> meterPointIds = records.stream()
            .map(r -> r.value())
            .filter(Objects::nonNull)
            .map(CmisReadingEvent::getMeterPointId)
            .filter(Objects::nonNull)
            .collect(Collectors.toSet());

        // Local cache map for max register values
        Map<String, BigDecimal> maxRegisters = new HashMap<>();
        if (!meterPointIds.isEmpty()) {
            String placeholders = String.join(",", Collections.nCopies(meterPointIds.size(), "?"));
            String correctSql = "SELECT mp.meter_point_id, COALESCE(mm.max_register_value, 99999.9) " +
                                "FROM meter_point mp " +
                                "LEFT JOIN meter_model mm ON mp.model_code = mm.model_code " +
                                "WHERE mp.meter_point_id IN (" + placeholders + ")";
            jdbcTemplate.query(correctSql, rs -> {
                maxRegisters.put(rs.getString(1), rs.getBigDecimal(2));
            }, meterPointIds.toArray());
        }

        List<Object[]> meterUsageBatch = new ArrayList<>();
        
        for (org.apache.kafka.clients.consumer.ConsumerRecord<String, CmisReadingEvent> record : records) {
            CmisReadingEvent event = record.value();
            if (event == null) continue;

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

            long generatedId = Math.abs((event.getMeterPointId() + "_" + month + "_" + period).hashCode());
            BigDecimal maxVal = maxRegisters.getOrDefault(event.getMeterPointId(), new BigDecimal("99999.9"));
            
            boolean isRollover = event.getEndIndex().compareTo(event.getStartIndex()) < 0;
            BigDecimal rawConsumption;
            if (isRollover) {
                rawConsumption = maxVal.subtract(event.getStartIndex()).add(event.getEndIndex());
            } else {
                rawConsumption = event.getEndIndex().subtract(event.getStartIndex());
            }

            LocalDateTime fromDate = event.getFromDate() != null ? event.getFromDate() : LocalDateTime.now().minusDays(30);
            LocalDateTime toDate = event.getToDate() != null ? event.getToDate() : LocalDateTime.now();

            String status = "VALIDATED";
            String reason = null;

            // Perform quality validation checks
            if (event.getEndIndex().compareTo(event.getStartIndex()) < 0 && !isRollover) {
                status = "PENDING_MANUAL";
                reason = "Index value dropped without hardware rollover capability.";
            } else if (rawConsumption.compareTo(new BigDecimal("5000.00")) > 0) {
                status = "SUSPECT";
                reason = "Consumption spike warnings (exceeds 5000 kWh limit).";
            }

            // [Nghiệp vụ Phân biệt Chỉ số đo xa hàng ngày và Chỉ số chốt kỳ cước]
            if ("VALIDATED".equals(status)) {
                BillingConfigSnapshot config = getSnapshotConfig(event.getAccountId(), month, period);
                if (config != null && config.getPeriodToDate() != null && event.getToDate() != null) {
                    // Nếu thời điểm đọc nhỏ hơn ngày chốt kỳ cước đã cấu hình -> chỉ ghi nhận làm chỉ số đo xa đo đếm
                    if (event.getToDate().toLocalDate().isBefore(config.getPeriodToDate())) {
                        status = "TELEMETRY"; // Chỉ số đo đếm hàng ngày
                    }
                }
            }

            // Publish exception event to CMIS validation results topic if validation fails
            if (!"VALIDATED".equals(status) && !"TELEMETRY".equals(status)) {
                try {
                    Map<String, Object> validationError = new HashMap<>();
                    validationError.put("bookId", "SO_01");
                    validationError.put("accountId", event.getAccountId());
                    validationError.put("meterPointId", event.getMeterPointId());
                    validationError.put("billingCycleMonth", event.getBillingCycleMonth());
                    validationError.put("usageId", generatedId);
                    validationError.put("status", status);
                    validationError.put("reason", reason);
                    validationError.put("startIndex", event.getStartIndex());
                    validationError.put("endIndex", event.getEndIndex());
                    validationError.put("timestamp", LocalDateTime.now().toString());

                    String validationErrorJson = objectMapper.writeValueAsString(validationError);
                    kafkaTemplate.send("meter-reading-validation-results", event.getAccountId(), validationErrorJson);
                    log.warn("[VALIDATION] Flagged anomalous reading for Account: {}, Status: {}, Reason: {}",
                            event.getAccountId(), status, reason);
                } catch (Exception e) {
                    log.error("Failed to publish validation error event to Kafka: {}", e.getMessage());
                }
            }

            // INSERT INTO meter_usage with sub_reading_seq = 1 (ORIGINAL)
            meterUsageBatch.add(new Object[] {
                generatedId,
                1, // sub_reading_seq
                event.getAccountId(),
                event.getMeterPointId(),
                month,
                period,
                java.sql.Timestamp.valueOf(fromDate),
                java.sql.Timestamp.valueOf(toDate),
                event.getStartIndex(),
                event.getEndIndex(),
                isRollover,
                maxVal,
                rawConsumption,
                status
            });
        }

        // 1. Bulk insert usages using JDBC Batch (ON CONFLICT DO NOTHING for strict Deduplication)
        if (!meterUsageBatch.isEmpty()) {
            String sql = "INSERT INTO meter_usage (" +
                    "usage_id, sub_reading_seq, account_id, meter_point_id, billing_cycle_month, period, " +
                    "from_date, to_date, start_index, end_index, is_rollover, " +
                    "max_register_snapshot, raw_consumption, status, record_type, source) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'ORIGINAL', 'AMR') " +
                    "ON CONFLICT DO NOTHING";
            jdbcTemplate.batchUpdate(sql, meterUsageBatch);
            log.info("Ingested batch of {} readings to DB via JDBC Batch (Deduplicated).", meterUsageBatch.size());
        }

        // 2. Perform completeness check and trigger calculation task only for final billing index readings
        for (org.apache.kafka.clients.consumer.ConsumerRecord<String, CmisReadingEvent> record : records) {
            CmisReadingEvent event = record.value();
            if (event == null) continue;

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

            // Check if it is the final billing index of the period
            BillingConfigSnapshot config = getSnapshotConfig(event.getAccountId(), month, period);
            boolean isBillingReading = true;
            if (config != null && config.getPeriodToDate() != null && event.getToDate() != null) {
                if (event.getToDate().toLocalDate().isBefore(config.getPeriodToDate())) {
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
                checkAndTriggerBilling(event.getAccountId(), month, period, event.getMeterPointId(), t1Ingest);
            }
        }
    }

    public void checkAndTriggerBilling(String accountId, String month, int period, String currentMeterPointId, long t1Ingest) {
        log.info("[INGESTION] Triggering billing check for Account: {}, Month: {}, Period: {}", accountId, month, period);
        
        BillingConfigSnapshot config = getSnapshotConfig(accountId, month, period);
        if (config == null) {
            log.warn("[INGESTION] Snapshot missing for Account: {}, Month: {}, Period: {}", accountId, month, period);
            return;
        }

        String bookId = config.getBookId() != null ? config.getBookId() : "SO_01";

        if (config.getMeterTopology() == null || config.getMeterTopology().getRootPoints() == null) {
            log.warn("[INGESTION] Topology is null or root points null for Account: {}", accountId);
            return;
        }

        // Collect all meter IDs in the topology
        Set<String> requiredMeters = new HashSet<>();
        for (MeterPointNode root : config.getMeterTopology().getRootPoints()) {
            collectMeterIds(root, requiredMeters);
        }

        // 3. Completeness check
        boolean isComplete = false;
        Set<String> receivedMeters = new HashSet<>();
        
        // Optimization: if there's only 1 meter required for this account, and it matches the current event, it's immediately complete!
        if (requiredMeters.size() == 1 && requiredMeters.contains(currentMeterPointId)) {
            MeterUsageId compositeKey = new MeterUsageId((long) Math.abs((currentMeterPointId + "_" + month + "_" + period).hashCode()), 1, month, period);
            Optional<MeterUsage> usageOpt = meterUsageRepository.findById(compositeKey);
            if (usageOpt.isPresent() && "VALIDATED".equals(usageOpt.get().getStatus())) {
                isComplete = true;
                receivedMeters.add(currentMeterPointId);
            }
        } else {
            // Fallback to database check for multi-meter accounts
            List<MeterUsage> validatedUsages = meterUsageRepository.findByAccountIdAndBillingCycleMonthAndPeriodAndStatus(accountId, month, period, "VALIDATED");
            for (MeterUsage u : validatedUsages) {
                receivedMeters.add(u.getMeterPointId());
            }
            if (receivedMeters.containsAll(requiredMeters)) {
                isComplete = true;
            }
        }

        log.info("[AUDIT-TRACER] [Account: {}] Step 3: Topology readiness check. Required meters: {}, Received meters: {}.", accountId, requiredMeters, receivedMeters);

        if (isComplete) {
            log.info("[AUDIT-TRACER] [Account: {}] Step 3.1: Netting readiness verified. Triggering calculation task via Kafka.", accountId);
            
            // Send billing task to Kafka
            BillingTaskDto task = new BillingTaskDto();
            task.setAccountId(accountId);
            task.setBookId(bookId);
            task.setBillingCycleMonth(month);
            task.setPeriod(period);
            task.setCalculationVersion(1);
            task.setTraceId(UUID.randomUUID().toString().replace("-", ""));
            
            List<MeterUsage> validatedUsages = meterUsageRepository.findByAccountIdAndBillingCycleMonthAndPeriodAndStatus(accountId, month, period, "VALIDATED");
            List<MeterReadingDto> readings = validatedUsages.stream()
                .map(u -> new MeterReadingDto(
                    u.getMeterPointId(),
                    u.getFromDate(),
                    u.getToDate(),
                    u.getStartIndex(),
                    u.getEndIndex(),
                    u.getConsumption(),
                    u.getIsRollover(),
                    u.getMaxRegisterSnapshot(),
                    u.getSubReadingSeq(),
                    u.getRecordType()
                ))
                .collect(Collectors.toList());
            task.setReadings(readings);

            long t2Send = System.currentTimeMillis();
            ProducerRecord<String, Object> producerRecord = new ProducerRecord<>("billing-execution-topic", accountId, task);
            producerRecord.headers().add("t1_ingest", ByteBuffer.allocate(8).putLong(t1Ingest).array());
            producerRecord.headers().add("t2_send", ByteBuffer.allocate(8).putLong(t2Send).array());

            kafkaTemplate.send(producerRecord);
            log.info("[INGESTION] Successfully sent billing task to billing-execution-topic for Account: {}", accountId);
        } else {
            log.info("[INGESTION] Account {} is missing readings. Required: {}, Received: {}", accountId, requiredMeters, receivedMeters);
        }
    }

    private void collectMeterIds(MeterPointNode node, Set<String> ids) {
        ids.add(node.getMeterPointId());
        if (node.getChildPoints() != null) {
            for (MeterPointNode child : node.getChildPoints()) {
                collectMeterIds(child, ids);
            }
        }
    }
}
