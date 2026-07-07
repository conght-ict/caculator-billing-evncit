package com.evn.billing.common.domain;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;
import java.time.LocalDate;
import java.time.LocalDateTime;

@Entity
@Table(name = "book_billing_schedule")
@IdClass(BookBillingScheduleId.class)
@Getter
@Setter
public class BookBillingSchedule {

    @Id
    @Column(name = "book_id", length = 50)
    private String bookId;

    @Id
    @Column(name = "billing_cycle_month", length = 20)
    private String billingCycleMonth;

    @Id
    @Column(name = "period")
    private Integer period = 1;

    @Column(name = "from_date", nullable = false)
    private LocalDate fromDate;

    @Column(name = "to_date", nullable = false)
    private LocalDate toDate;

    @Column(name = "status", length = 20, nullable = false)
    private String status = "ACTIVE"; // ACTIVE, CLOSED

    @Column(name = "run_status", length = 20, nullable = false)
    private String runStatus = "PENDING"; // PENDING, SNAPSHOT_GENERATING, PROCESSING, COMPLETED, FAILED

    @Column(name = "total_accounts")
    private int totalAccounts = 0;

    @Column(name = "processed_accounts")
    private int processedAccounts = 0;

    @Column(name = "success_accounts")
    private int successAccounts = 0;

    @Column(name = "failed_accounts")
    private int failedAccounts = 0;

    @Column(name = "triggered_by", length = 20)
    private String triggeredBy = "CMIS";

    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt = LocalDateTime.now();

    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt = LocalDateTime.now();
}
