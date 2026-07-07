package com.evn.billing.common.domain;

import jakarta.persistence.*;
import lombok.Data;
import java.math.BigDecimal;

@Entity
@Table(name = "tariff_detail")
@Data
public class TariffDetail {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "detail_id")
    private Long detailId;

    @Column(name = "tariff_code", length = 50, nullable = false)
    private String tariffCode;

    @Column(name = "step", nullable = false)
    private Integer step;

    @Column(name = "min_kwh", nullable = false, precision = 12, scale = 2)
    private BigDecimal minKwh;

    @Column(name = "max_kwh", precision = 12, scale = 2)
    private BigDecimal maxKwh; // Can be null for the final tier block

    @Column(name = "unit_price", nullable = false, precision = 12, scale = 2)
    private BigDecimal unitPrice;

    @Column(name = "tou_period", length = 20)
    private String touPeriod; // PEAK, OFF_PEAK, NORMAL
}
