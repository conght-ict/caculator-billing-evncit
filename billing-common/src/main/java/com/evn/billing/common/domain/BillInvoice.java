package com.evn.billing.common.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;
import java.math.BigDecimal;
import java.time.LocalDateTime;

@Entity
@Table(name = "bill_invoice")
@IdClass(BillInvoiceId.class)
@Getter
@Setter
public class BillInvoice {

    @Id
    @Column(name = "invoice_id", length = 50)
    private String invoiceId;

    @Id
    @Column(name = "billing_cycle_month", length = 10)
    private String billingCycleMonth; // Format: YYYY_MM

    @Column(name = "account_id", length = 50, nullable = false)
    private String accountId;

    @Column(name = "book_id", length = 20, nullable = false)
    private String bookId;

    @Column(name = "total_amount_before_tax", nullable = false, precision = 15, scale = 2)
    private BigDecimal totalAmountBeforeTax;

    @Column(name = "tax_amount", nullable = false, precision = 15, scale = 2)
    private BigDecimal taxAmount;

    @Column(name = "total_amount_after_tax", nullable = false, precision = 15, scale = 2)
    private BigDecimal totalAmountAfterTax;

    @Column(name = "idempotency_key", length = 100, nullable = false)
    private String idempotencyKey;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "billing_manifest", nullable = false)
    private String billingManifest; // Stored as JSONB in DB

    @Column(name = "created_at", nullable = false)
    private LocalDateTime createdAt = LocalDateTime.now();
}
