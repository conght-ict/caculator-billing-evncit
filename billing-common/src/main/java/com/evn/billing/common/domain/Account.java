package com.evn.billing.common.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;

@Entity
@Table(name = "account")
@Getter
@Setter
public class Account {

    @Id
    @Column(name = "account_id", length = 50)
    private String accountId;

    @Column(name = "book_id", length = 20, nullable = false)
    private String bookId;

    @Column(name = "customer_name", length = 100, nullable = false)
    private String customerName;

    @Column(name = "status", length = 20, nullable = false)
    private String status = "ACTIVE"; // ACTIVE, SUSPENDED

    @Column(name = "norms_factor", nullable = false)
    private int normsFactor = 1; // Định mức số hộ dùng chung
}
