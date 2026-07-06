package com.evn.billing.common.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;
import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = "book_billing_run")
@Getter
@Setter
public class BookBillingRun {

    @Id
    @Column(name = "run_id")
    private UUID runId;

    @Column(name = "book_id", length = 50, nullable = false)
    private String bookId;

    @Column(name = "billing_cycle_month", length = 20, nullable = false)
    private String billingCycleMonth;

    @Column(name = "status", length = 20, nullable = false)
    private String status; // INITIATED, PROCESSING, COMPLETED, FAILED

    @Column(name = "total_accounts")
    private int totalAccounts = 0;

    @Column(name = "processed_accounts")
    private int processedAccounts = 0;

    @Column(name = "success_accounts")
    private int successAccounts = 0;

    @Column(name = "failed_accounts")
    private int failedAccounts = 0;

    @Column(name = "created_at")
    private LocalDateTime createdAt = LocalDateTime.now();

    @Column(name = "updated_at")
    private LocalDateTime updatedAt = LocalDateTime.now();
}
