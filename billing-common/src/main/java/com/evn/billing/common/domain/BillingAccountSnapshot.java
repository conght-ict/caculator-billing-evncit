package com.evn.billing.common.domain;

import com.evn.billing.common.dto.BillingConfigSnapshot;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;
import java.time.LocalDateTime;

@Entity
@Table(name = "billing_account_snapshot")
@Getter
@Setter
public class BillingAccountSnapshot {

    @Id
    @Column(name = "snapshot_id", length = 64)
    private String snapshotId; // account_id + billing_cycle_month + calculation_version

    @Column(name = "account_id", length = 50, nullable = false)
    private String accountId;

    @Column(name = "book_id", length = 20, nullable = false)
    private String bookId;

    @Column(name = "billing_cycle_month", length = 10, nullable = false)
    private String billingCycleMonth;

    @Column(name = "calculation_version", nullable = false)
    private Integer calculationVersion = 1;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "config_data", nullable = false)
    private BillingConfigSnapshot configData;

    @Column(name = "created_at", nullable = false)
    private LocalDateTime createdAt = LocalDateTime.now();
}
