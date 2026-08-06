package com.evn.billing.common.dto;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class TariffConfig {
    private int soThuTu;
    private String maNhomnn;
    private String maNn;
    private String maCapda;
    private String maNgia;
    private String tgianBdien; // BT, CD, TD, KT
    private BigDecimal dinhMuc;
    private String loaiDmuc; // TL, SL
    private int soHo = 1;
}
