package com.evn.billing.common.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.Table;
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
    @Column(name = "billing_cycle_month", length = 10)
    private String billingCycleMonth; // Format: YYYY_MM

    @Column(name = "account_id", length = 50, nullable = false)
    private String accountId;

    @Column(name = "meter_point_id", length = 50, nullable = false)
    private String meterPointId;

    @Column(name = "from_date", nullable = false)
    private LocalDateTime fromDate;

    @Column(name = "to_date", nullable = false)
    private LocalDateTime toDate;

    @Column(name = "start_index", nullable = false, precision = 12, scale = 2)
    private BigDecimal startIndex;

    @Column(name = "end_index", nullable = false, precision = 12, scale = 2)
    private BigDecimal endIndex;

    // GENERATED ALWAYS columns are computed by DB. In JPA, mark them as read-only.
    @Column(name = "consumption", insertable = false, updatable = false, precision = 12, scale = 2)
    private BigDecimal consumption;

    @Column(name = "status", length = 20, nullable = false)
    private String status = "PENDING_MANUAL"; // VALIDATED, PENDING_MANUAL
}
