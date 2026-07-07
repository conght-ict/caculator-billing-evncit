package com.evn.billing.common.domain;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;
import java.math.BigDecimal;
import java.time.LocalDateTime;

@Entity
@Table(name = "meter_usage")
@IdClass(MeterUsageId.class)
@Getter
@Setter
public class MeterUsage {

    @Id
    @Column(name = "usage_id")
    private Long usageId;

    @Id
    @Column(name = "sub_reading_seq")
    private Integer subReadingSeq = 1;

    @Id
    @Column(name = "billing_cycle_month", length = 20)
    private String billingCycleMonth; // Format: YYYY_MM

    @Id
    @Column(name = "period")
    private Integer period = 1;

    @Column(name = "account_id", length = 50, nullable = false)
    private String accountId;

    @Column(name = "meter_point_id", length = 50, nullable = false)
    private String meterPointId;

    @Column(name = "from_date", nullable = false)
    private LocalDateTime fromDate;

    @Column(name = "to_date", nullable = false)
    private LocalDateTime toDate;

    @Column(name = "start_index", nullable = false, precision = 14, scale = 2)
    private BigDecimal startIndex;

    @Column(name = "end_index", nullable = false, precision = 14, scale = 2)
    private BigDecimal endIndex;

    @Column(name = "is_rollover", nullable = false)
    private Boolean isRollover = false;

    @Column(name = "max_register_snapshot", precision = 14, scale = 2)
    private BigDecimal maxRegisterSnapshot;

    @Column(name = "raw_consumption", nullable = false, precision = 14, scale = 2)
    private BigDecimal rawConsumption;

    @Column(name = "status", length = 20, nullable = false)
    private String status = "PENDING_MANUAL"; // VALIDATED, PENDING_MANUAL

    @Column(name = "record_type", length = 20, nullable = false)
    private String recordType = "ORIGINAL"; // ORIGINAL, CORRECTION

    @Column(name = "correction_of_usage_id")
    private Long correctionOfUsageId;

    @Column(name = "source", length = 20, nullable = false)
    private String source = "AMR"; // AMR, HANDHELD, MANUAL

    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt = LocalDateTime.now();

    // Compatibility methods for existing codebase which queries getConsumption/setConsumption
    public BigDecimal getConsumption() {
        return rawConsumption;
    }

    public void setConsumption(BigDecimal consumption) {
        this.rawConsumption = consumption;
    }
}
