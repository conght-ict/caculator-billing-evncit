package com.evn.billing.mediation.controller;

import com.evn.billing.common.domain.MeterUsage;
import com.evn.billing.common.domain.MeterUsageId;
import com.evn.billing.mediation.repository.MeterUsageRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;

@RestController
@RequestMapping("/api/v1")
public class ExceptionPortalController {

    @Autowired
    private MeterUsageRepository meterUsageRepository;

    @GetMapping("/exceptions")
    public ResponseEntity<List<MeterUsage>> getExceptions(@RequestParam String month) {
        List<MeterUsage> exceptions = meterUsageRepository.findPendingManualByMonth(month);
        return ResponseEntity.ok(exceptions);
    }

    @PostMapping("/exceptions/resolve")
    public ResponseEntity<String> resolveException(@RequestBody ResolveRequest request) {
        MeterUsageId compositeKey = new MeterUsageId(request.getUsageId(), request.getBillingCycleMonth());
        Optional<MeterUsage> usageOpt = meterUsageRepository.findById(compositeKey);

        if (usageOpt.isEmpty()) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body("Meter usage record not found.");
        }

        MeterUsage usage = usageOpt.get();
        usage.setEndIndex(request.getCorrectedEndIndex());
        usage.setStatus("VALIDATED"); // Phê duyệt thủ công, cập nhật trạng thái hợp lệ

        meterUsageRepository.save(usage);
        return ResponseEntity.ok("Exception resolved and validated.");
    }

    public static class ResolveRequest {
        private Long usageId;
        private String billingCycleMonth;
        private BigDecimal correctedEndIndex;

        public Long getUsageId() { return usageId; }
        public void setUsageId(Long usageId) { this.usageId = usageId; }
        public String getBillingCycleMonth() { return billingCycleMonth; }
        public void setBillingCycleMonth(String billingCycleMonth) { this.billingCycleMonth = billingCycleMonth; }
        public BigDecimal getCorrectedEndIndex() { return correctedEndIndex; }
        public void setCorrectedEndIndex(BigDecimal correctedEndIndex) { this.correctedEndIndex = correctedEndIndex; }
    }
}
