package com.evn.billing.mediation.dto;

import lombok.Data;
import java.math.BigDecimal;

@Data
public class ReadingResolutionEvent {
    private String loaiXuLy; // ACCEPT_AS_IS, CORRECT
    @com.fasterxml.jackson.annotation.JsonAlias("accountId")
    private String maKhang;
    @com.fasterxml.jackson.annotation.JsonAlias("billingCycleMonth")
    private String thangChuKy;
    @com.fasterxml.jackson.annotation.JsonAlias({"bookId", "maSogcs", "dtuongQly"})
    private String dtuongQly;
    @com.fasterxml.jackson.annotation.JsonAlias("usageId")
    private Long idChiSo;
    @com.fasterxml.jackson.annotation.JsonAlias({"correctedEndIndex", "correctedIndex"})
    private BigDecimal chiSoCuoiDieuChinh;
}
