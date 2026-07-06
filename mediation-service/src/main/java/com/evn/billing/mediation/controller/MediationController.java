package com.evn.billing.mediation.controller;

import com.evn.billing.common.domain.MeterUsage;
import com.evn.billing.mediation.repository.MeterUsageRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
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

    @PostMapping("/readings")
    public ResponseEntity<String> receiveReadings(@RequestBody List<ReadingDto> readings) {
        List<MeterUsage> batch = new ArrayList<>();
        Random random = new Random();

        for (ReadingDto dto : readings) {
            // Lọc trùng (Deduplication) - Tìm kiếm bản ghi cũ
            Optional<MeterUsage> existingOpt = meterUsageRepository
                    .findByAccountIdAndMeterPointIdAndBillingCycleMonth(
                            dto.getAccountId(), dto.getMeterPointId(), dto.getBillingCycleMonth());

            MeterUsage usage = existingOpt.orElseGet(MeterUsage::new);
            
            // Nếu là bản ghi mới, sinh ngẫu nhiên usage_id
            if (usage.getUsageId() == null) {
                usage.setUsageId(Math.abs(random.nextLong()));
            }

            usage.setAccountId(dto.getAccountId());
            usage.setMeterPointId(dto.getMeterPointId());
            usage.setBillingCycleMonth(dto.getBillingCycleMonth());
            usage.setFromDate(dto.getFromDate());
            usage.setToDate(dto.getToDate());
            usage.setStartIndex(dto.getStartIndex());
            usage.setEndIndex(dto.getEndIndex());

            // Kiểm soát lỗi nhập chỉ số: Chỉ số cuối kì không được nhỏ hơn chỉ số đầu kì
            if (dto.getEndIndex().compareTo(dto.getStartIndex()) < 0) {
                usage.setStatus("PENDING_MANUAL"); // Đánh dấu lỗi để nhập sửa tay
            } else {
                usage.setStatus("VALIDATED");       // Hợp lệ
            }

            batch.add(usage);
        }

        meterUsageRepository.saveAll(batch);
        return ResponseEntity.status(HttpStatus.ACCEPTED).body("Readings processed and saved.");
    }

    public static class ReadingDto {
        private String accountId;
        private String meterPointId;
        private String billingCycleMonth;
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
