package com.evn.billing.common.domain;

import jakarta.persistence.*;
import lombok.Data;

@Entity
@Table(name = "meter_point")
@Data
public class MeterPoint {

    @Id
    @Column(name = "meter_point_id", length = 50)
    private String meterPointId;

    @Column(name = "account_id", length = 50, nullable = false)
    private String accountId;

    @Column(name = "tariff_code", length = 50, nullable = false)
    private String tariffCode;

    @Column(name = "status", length = 20, nullable = false)
    private String status; // ACTIVE, INACTIVE
}
