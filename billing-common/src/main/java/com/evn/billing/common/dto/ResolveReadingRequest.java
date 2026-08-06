package com.evn.billing.common.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.math.BigDecimal;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ResolveReadingRequest {
    private String loaiXuLy;
    private String maKhang;
    private String thangChuKy;
    private String dtuongQly;
    private Long idChiSo;
    private BigDecimal chiSoCuoiDieuChinh;
}
