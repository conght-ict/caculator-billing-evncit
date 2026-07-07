package com.evn.billing.common.domain;

import jakarta.persistence.*;
import lombok.Data;
import java.time.LocalDate;

@Entity
@Table(name = "meter_point")
@Data
public class MeterPoint {

    @Id
    @Column(name = "meter_point_id", length = 50)
    private String meterPointId;

    @Column(name = "account_id", length = 50, nullable = false)
    private String accountId;

    @Column(name = "model_code", length = 50)
    private String modelCode;

    @Column(name = "tariff_code", length = 50, nullable = false)
    private String tariffCode;

    @Column(name = "status", length = 20, nullable = false)
    private String status; // ACTIVE, INACTIVE

    @Column(name = "meter_serial", length = 100)
    private String meterSerial;

    @Column(name = "installed_date")
    private LocalDate installedDate;

    @Column(name = "decommission_date")
    private LocalDate decommissionDate;
}
