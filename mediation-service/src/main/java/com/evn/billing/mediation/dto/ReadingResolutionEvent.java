package com.evn.billing.mediation.dto;

import lombok.Data;
import java.math.BigDecimal;

@Data
public class ReadingResolutionEvent {
    private String loaiXuLy; // ACCEPT_AS_IS, CORRECT
    private String maKhang;
    private String thangChuKy;
    private String dtuongQly;
    private Long idChiSo;
    private BigDecimal chiSoCuoiDieuChinh;
}
