package com.evn.billing.mediation.controller;

import com.evn.billing.common.domain.MeterUsage;
import com.evn.billing.common.domain.MeterUsageId;
import com.evn.billing.mediation.repository.MeterUsageRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;

@RestController
@RequestMapping("/api/v1")
public class MediationController {

    @Autowired
    private MeterUsageRepository meterUsageRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @PostMapping("/readings")
    public ResponseEntity<String> receiveReadings(@RequestBody List<ReadingDto> readings) {
        List<MeterUsage> batch = new ArrayList<>();
        Random random = new Random();

        for (ReadingDto dto : readings) {
            int effectivePeriod = dto.getPeriod() != null ? dto.getPeriod() : 1;
            // Check if there is already a record to prevent duplication
            long generatedId = Math.abs((dto.getMeterPointId() + "_" + dto.getBillingCycleMonth()).hashCode());
            MeterUsageId compositeKey = new MeterUsageId(generatedId, 1, dto.getBillingCycleMonth(), effectivePeriod);
            Optional<MeterUsage> existingOpt = meterUsageRepository.findById(compositeKey);

            if (existingOpt.isPresent()) {
                // Skip if duplicate (Strict Idempotency/Deduplication)
                continue;
            }

            MeterUsage usage = new MeterUsage();
            usage.setUsageId(generatedId);
            usage.setSubReadingSeq(1); // ORIGINAL
            usage.setPeriod(effectivePeriod);
            usage.setAccountId(dto.getAccountId());
            usage.setMeterPointId(dto.getMeterPointId());
            usage.setBillingCycleMonth(dto.getBillingCycleMonth());
            usage.setFromDate(dto.getFromDate());
            usage.setToDate(dto.getToDate());
            usage.setStartIndex(dto.getStartIndex());
            usage.setEndIndex(dto.getEndIndex());

            // Fetch max register value for this meter point if possible, otherwise default
            BigDecimal maxVal = new BigDecimal("99999.9");
            try {
                BigDecimal dbMax = jdbcTemplate.queryForObject(
                    "SELECT COALESCE(mm.max_register_value, 99999.9) FROM meter_point mp " +
                    "LEFT JOIN meter_model mm ON mp.model_code = mm.model_code " +
                    "WHERE mp.meter_point_id = ?", BigDecimal.class, dto.getMeterPointId());
                if (dbMax != null) {
                    maxVal = dbMax;
                }
            } catch (Exception e) {
                // Fallback to default
            }

            usage.setMaxRegisterSnapshot(maxVal);
            boolean isRollover = dto.getEndIndex().compareTo(dto.getStartIndex()) < 0;
            usage.setIsRollover(isRollover);

            if (isRollover) {
                usage.setRawConsumption(maxVal.subtract(dto.getStartIndex()).add(dto.getEndIndex()));
                usage.setStatus("VALIDATED"); // Validated because it satisfies hardware rollover
            } else {
                usage.setRawConsumption(dto.getEndIndex().subtract(dto.getStartIndex()));
                usage.setStatus("VALIDATED");
            }

            usage.setRecordType("ORIGINAL");
            usage.setSource("AMR");
            usage.setCreatedAt(LocalDateTime.now());

            batch.add(usage);
        }

        if (!batch.isEmpty()) {
            meterUsageRepository.saveAll(batch);
        }
        return ResponseEntity.status(HttpStatus.ACCEPTED).body("Readings processed and saved.");
    }

    public static class ReadingDto {
        private String accountId;
        private String meterPointId;
        private String billingCycleMonth;
        private Integer period = 1;
        private LocalDateTime fromDate;
        private LocalDateTime toDate;
        private BigDecimal startIndex;
        private BigDecimal endIndex;

        public String getAccountId() { return accountId; }
        public void setAccountId(String accountId) { this.accountId = accountId; }
        public String getMeterPointId() { return meterPointId; }
        public void setMeterPointId(String meterPointId) { this.meterPointId = meterPointId; }
        public String getBillingCycleMonth() { return billingCycleMonth; }
        public void setBillingCycleMonth(String billingCycleMonth) { this.billingCycleMonth = billingCycleMonth; }
        public Integer getPeriod() { return period; }
        public void setPeriod(Integer period) { this.period = period; }
        public LocalDateTime getFromDate() { return fromDate; }
        public void setFromDate(LocalDateTime fromDate) { this.fromDate = fromDate; }
        public LocalDateTime getToDate() { return toDate; }
        public void setToDate(LocalDateTime toDate) { this.toDate = toDate; }
        public BigDecimal getStartIndex() { return startIndex; }
        public void setStartIndex(BigDecimal startIndex) { this.startIndex = startIndex; }
        public BigDecimal getEndIndex() { return endIndex; }
        public void setEndIndex(BigDecimal endIndex) { this.endIndex = endIndex; }
    }
}
