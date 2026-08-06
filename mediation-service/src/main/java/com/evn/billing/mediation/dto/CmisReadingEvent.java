package com.evn.billing.mediation.dto;

import lombok.Data;
import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
public class CmisReadingEvent {
    private String maKhang;
    private String maDdo;
    private BigDecimal chiSoDau;
    private BigDecimal chiSoCuoi;
    private String thangChuKy; // Format: YYYY_MM_Period (e.g. 2026_06_1)
    private LocalDateTime tuNgay;
    private LocalDateTime denNgay;
    private String maCto;
    private Integer soLanQuayVong = 1;
    private String nguonGhi = "AMR";
    private String tgianBdien = "BT";
}
