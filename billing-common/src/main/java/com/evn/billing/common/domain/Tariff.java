package com.evn.billing.common.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;

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
    private String type; // STEPPING, FLAT
}
