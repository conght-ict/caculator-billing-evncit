package com.evn.billing.common.dto;

import lombok.Data;
import java.math.BigDecimal;

@Data
public class TariffBlock {
    private int soThuTu;
    private BigDecimal minKwh;
    private BigDecimal maxKwh; // Can be null for the last tier
    private BigDecimal donGia;
    private String tgianBdien; // PEAK, OFF_PEAK, NORMAL (hoặc BT, CD, TD)
}
