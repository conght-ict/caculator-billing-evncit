package com.evn.billing.common.dto;

import lombok.Data;
import java.time.LocalDate;
import java.util.List;

@Data
public class TariffRules {
    private String maNgia;
    private String loaiBieuGia; // STEPPING, FLAT, TOU
    private boolean bacThang;
    private java.math.BigDecimal donGiaPhang;
    private LocalDate ngayHieuLuc;
    private LocalDate ngayHetHan;
    private List<TariffBlock> blocks;
}
