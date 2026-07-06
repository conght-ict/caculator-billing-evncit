package com.evn.billing.common.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;
import java.time.LocalDateTime;

@Entity
@Table(name = "account_billing_status")
@IdClass(AccountBillingStatusId.class)
@Getter
@Setter
public class AccountBillingStatus {

    @Id
    @Column(name = "account_id", length = 50)
    private String accountId;

    @Id
    @Column(name = "billing_cycle_month", length = 20)
    private String billingCycleMonth;

    @Column(name = "book_id", length = 50, nullable = false)
    private String bookId;

    @Column(name = "status", length = 20, nullable = false)
    private String status; // SUCCESS, FAILED, CANCELLED

    @Column(name = "invoice_id", length = 100)
    private String invoiceId;

    @Column(name = "error_message")
    private String errorMessage;

    @Column(name = "updated_at")
    private LocalDateTime updatedAt = LocalDateTime.now();
}
