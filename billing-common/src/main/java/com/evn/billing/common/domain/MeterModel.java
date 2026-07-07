package com.evn.billing.common.domain;

import jakarta.persistence.*;
import lombok.Data;
import java.math.BigDecimal;
import java.time.LocalDateTime;

@Entity
@Table(name = "meter_model")
@Data
public class MeterModel {

    @Id
    @Column(name = "model_code", length = 50)
    private String modelCode;

    @Column(name = "manufacturer", length = 100, nullable = false)
    private String manufacturer;

    @Column(name = "model_name", length = 100, nullable = false)
    private String modelName;

    @Column(name = "max_register_value", nullable = false, precision = 14, scale = 2)
    private BigDecimal maxRegisterValue = new BigDecimal("99999.9");

    @Column(name = "display_digits", nullable = false)
    private Integer displayDigits = 5;

    @Column(name = "meter_type", length = 20, nullable = false)
    private String meterType = "MECHANICAL"; // MECHANICAL, ELECTRONIC, SMART_AMI

    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt = LocalDateTime.now();
}
