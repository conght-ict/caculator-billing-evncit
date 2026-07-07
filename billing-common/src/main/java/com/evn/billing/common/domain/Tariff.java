package com.evn.billing.common.domain;

import jakarta.persistence.*;
import lombok.Data;
import java.time.LocalDate;
import java.time.LocalDateTime;

@Entity
@Table(name = "tariff")
@Data
public class Tariff {

    @Id
    @Column(name = "tariff_code", length = 50)
    private String tariffCode;

    @Column(name = "name", length = 100, nullable = false)
    private String name;

    @Column(name = "type", length = 20, nullable = false)
    private String type; // STEPPING, FLAT, TOU

    @Column(name = "effective_date", nullable = false)
    private LocalDate effectiveDate;

    @Column(name = "expiry_date")
    private LocalDate expiryDate;

    @Column(name = "issued_by", length = 300)
    private String issuedBy;

    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt = LocalDateTime.now();
}
